package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/melonly/sirocco/internal/config"
	"github.com/melonly/sirocco/internal/discord"
	"github.com/melonly/sirocco/internal/ratelimit"
	"github.com/melonly/sirocco/internal/validation"
)

type Upstream interface {
	Do(context.Context, *http.Request, []byte) (*http.Response, discord.Meta, error)
	Stats() discord.Stats
}

type Validator interface {
	Validate(context.Context, *http.Request, []byte) *validation.Error
	Stats(int) validation.Stats
}

type Server struct {
	cfg       config.Config
	log       *slog.Logger
	limiter   *ratelimit.Manager
	upstream  Upstream
	validator Validator
	started   time.Time
	http      *http.Server
}

func New(cfg config.Config, log *slog.Logger, limiter *ratelimit.Manager, upstream Upstream, validator Validator) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, log: log.With("component", "http"), limiter: limiter, upstream: upstream, validator: validator, started: time.Now()}
	s.http = &http.Server{Addr: cfg.ListenAddress, Handler: s, ReadHeaderTimeout: 5 * time.Second}
	return s
}

func (s *Server) Start() error {
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/_sirocco/health":
		s.health(w, r)
	case "/_sirocco/meta":
		s.meta(w, r)
	default:
		if strings.HasPrefix(r.URL.Path, "/_sirocco/") {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
			return
		}
		s.proxy(w, r)
	}
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method_not_allowed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) meta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method_not_allowed"})
		return
	}

	snap := s.limiter.Snapshot()
	body := struct {
		UptimeSeconds      float64           `json:"uptime_seconds"`
		ListenAddress      string            `json:"listen_address"`
		DiscordBaseURL     string            `json:"discord_base_url"`
		ValidationEnabled  bool              `json:"validation_enabled"`
		Validation         *validation.Stats `json:"validation,omitempty"`
		Upstream           discord.Stats     `json:"upstream"`
		Limiter            any               `json:"limiter"`
		MaxUpstreamRetries int               `json:"max_upstream_retries"`
		RetryBaseMillis    int64             `json:"retry_base_ms"`
		RetryMaxMillis     int64             `json:"retry_max_ms"`
		HTTP2Enabled       bool              `json:"http2_enabled"`
		MaxBodyBytes       int64             `json:"max_body_bytes"`
		StatePersistenceOn bool              `json:"state_persistence"`
		StatePath          string            `json:"state_path,omitempty"`
	}{
		UptimeSeconds:      time.Since(s.started).Seconds(),
		ListenAddress:      s.cfg.ListenAddress,
		DiscordBaseURL:     s.cfg.DiscordBaseURL,
		ValidationEnabled:  s.validator != nil,
		Upstream:           s.upstream.Stats(),
		Limiter:            snap,
		MaxUpstreamRetries: s.cfg.HTTP.RetryLimit,
		RetryBaseMillis:    s.cfg.HTTP.RetryBaseDelay.Milliseconds(),
		RetryMaxMillis:     s.cfg.HTTP.RetryMaxDelay.Milliseconds(),
		HTTP2Enabled:       !s.cfg.HTTP.DisableHTTP2,
		MaxBodyBytes:       s.cfg.MaxBodyBytes,
		StatePersistenceOn: s.cfg.StatePath != "",
		StatePath:          s.cfg.StatePath,
	}
	if s.validator != nil {
		stats := s.validator.Stats(10)
		body.Validation = &stats
	}

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(w, r, s.cfg.MaxBodyBytes)
	if err != nil {
		s.log.Debug("failed to read request body", "error", err, "path", r.URL.Path)
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": err.Error()})
		return
	}

	if s.validator != nil {
		if verr := s.validator.Validate(r.Context(), r, body); verr != nil {
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Sirocco-Validation", "blocked")
			w.WriteHeader(verr.Status)
			_, _ = w.Write(verr.Payload())
			return
		}
	}

	token := extractToken(r.Header.Get("Authorization"))
	plan := s.limiter.Plan(r.Method, r.URL.Path, token)
	waitStarted := time.Now()
	wait, release := s.limiter.Acquire(plan, token, waitStarted)
	if wait > 0 {
		if err := sleep(r.Context(), wait); err != nil {
			release(false, ratelimit.ReleaseMeta{})
			return
		}
	}
	waited := time.Since(waitStarted)

	upstreamStarted := time.Now()
	resp, meta, err := s.upstream.Do(r.Context(), r, body)
	if err != nil {
		release(false, ratelimit.ReleaseMeta{Headers: meta.Headers, Status: meta.Status})
		s.log.Warn("upstream request failed", "error", err, "method", r.Method, "path", r.URL.Path, "token", ratelimit.MaskToken(token))
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "bad_gateway", "message": "upstream request failed"})
		return
	}
	defer resp.Body.Close()

	release(resp.StatusCode < 500, ratelimit.ReleaseMeta{Headers: meta.Headers, Status: meta.Status})
	copyResponseHeaders(w.Header(), resp.Header)
	s.injectHeaders(w.Header(), plan, waited, wait, time.Since(upstreamStarted), meta)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (s *Server) injectHeaders(h http.Header, plan ratelimit.Plan, waited, planned, latency time.Duration, meta discord.Meta) {
	h.Set("X-Sirocco-Route", plan.Route)
	h.Set("X-Sirocco-Bucket-Key", plan.Key)
	h.Set("X-Sirocco-Waited", formatSeconds(waited))
	h.Set("X-Sirocco-Planned-Wait", formatSeconds(planned))
	h.Set("X-Sirocco-Upstream-Latency", formatSeconds(latency))
	h.Set("X-Sirocco-Upstream-Status", strconv.Itoa(meta.Status))
	if meta.Retries > 0 {
		h.Set("X-Sirocco-Upstream-Retries", strconv.Itoa(meta.Retries))
	}
}

func readBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	defer r.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, max))
	if err != nil {
		return nil, err
	}
	return body, nil
}

func extractToken(auth string) string {
	auth = strings.TrimSpace(auth)
	if auth == "" {
		return ""
	}
	if len(auth) >= 4 && strings.EqualFold(auth[:4], "bot ") {
		return strings.TrimSpace(auth[4:])
	}
	if len(auth) >= 7 && strings.EqualFold(auth[:7], "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return auth
}

func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		if hopHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func hopHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func formatSeconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', 6, 64)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

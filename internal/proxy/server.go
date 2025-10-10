package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/melonly/sirocco/internal/config"
	"github.com/melonly/sirocco/internal/ratelimit"
	"github.com/melonly/sirocco/internal/transport"
	"github.com/melonly/sirocco/internal/util"
)

type Server struct {
	cfg *config.Config
	log zerolog.Logger
	rl  *ratelimit.Manager
	dc  *transport.DiscordClient

	http    *http.Server
	started time.Time
}

func NewServer(cfg *config.Config, log zerolog.Logger, rl *ratelimit.Manager, dc *transport.DiscordClient) *Server {
	s := &Server{cfg: cfg, log: log, rl: rl, dc: dc, started: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	s.http = &http.Server{Addr: cfg.BindAddr + ":" + itoa(cfg.Port), Handler: mux}
	return s
}

func (s *Server) Start() error {
	return s.http.ListenAndServe()
}
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/_sirocco/health":
		s.handleHealth(w, r)
		return
	case "/_sirocco/meta":
		s.handleMeta(w, r)
		return
	default:
		if strings.HasPrefix(r.URL.Path, "/_sirocco/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}

	token := extractToken(r)
	s.log.Debug().Str("method", r.Method).Str("path", r.URL.Path).Str("token", maskToken(token)).Msg("incoming request")

	// buffer body for reuse across forward/upstream
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}
	r.Body = io.NopCloser(strings.NewReader(string(body)))

	_, bucketKey, route := s.rl.Plan(r.Method, r.URL.Path, token)
	s.handleUpstream(w, r, bucketKey, token, body, route)
}

func (s *Server) handleUpstream(w http.ResponseWriter, r *http.Request, key, token string, body []byte, route string) {
	// rate limit acquire
	acquireStart := time.Now()
	release, plannedWait := s.rl.AcquireWithRoute(key, token, route, acquireStart)
	if plannedWait > 0 {
		s.log.Debug().Dur("wait", plannedWait).Str("key", key).Msg("rate limit wait")
		time.Sleep(plannedWait)
	}
	waited := time.Since(acquireStart)
	// perform upstream request
	upstreamStart := time.Now()
	resp, hdr, err := s.dc.Do(r.Context(), r, body)
	upstreamLatency := time.Since(upstreamStart)
	release(err == nil && resp != nil && resp.StatusCode < 500, hdr)
	if err != nil {
		s.log.Error().Err(err).Str("method", r.Method).Str("path", r.URL.Path).Msg("upstream request failed")
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	util.CopyHeaders(w.Header(), resp.Header)
	s.injectResponseHeaders(w, key, route, waited, plannedWait, upstreamLatency, hdr)
	s.log.Debug().Dur("waited", waited).Dur("planned", plannedWait).Dur("upstream", upstreamLatency).Str("bucketKey", key).Str("route", route).Int("status", resp.StatusCode).Msg("served request")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func extractToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(auth), "bot ") {
		return strings.TrimSpace(auth[4:])
	}
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return auth
}

func itoa(n int) string { return strconv.Itoa(n) }

func maskToken(token string) string {
	if len(token) <= 4 {
		return token
	}
	return token[:4] + strings.Repeat("*", len(token)-4)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	snap := s.rl.Snapshot()
	meta := struct {
		UptimeSeconds       float64 `json:"uptime_seconds"`
		RouteCachePersisted bool    `json:"route_cache_persisted"`
		StatePath           string  `json:"state_path,omitempty"`
		Buckets             int     `json:"buckets"`
		Globals             int     `json:"globals"`
		Routes              int     `json:"routes"`
		InvalidEvents       int     `json:"invalid_events"`
		BotOverrides        int     `json:"bot_overrides"`
		MaxUpstreamRetries  int     `json:"max_upstream_retries"`
		RetryBaseMS         int     `json:"retry_base_ms"`
		RetryMaxMS          int     `json:"retry_max_ms"`
		HTTP2Enabled        bool    `json:"http2_enabled"`
	}{
		UptimeSeconds:       time.Since(s.started).Seconds(),
		RouteCachePersisted: s.cfg.StatePath != "",
		StatePath:           s.cfg.StatePath,
		Buckets:             snap.Buckets,
		Globals:             snap.Globals,
		Routes:              snap.Routes,
		InvalidEvents:       snap.InvalidEvents,
		BotOverrides:        len(s.cfg.GlobalOverride),
		MaxUpstreamRetries:  s.cfg.MaxUpstreamRetries,
		RetryBaseMS:         int(s.cfg.RetryBaseDelay / time.Millisecond),
		RetryMaxMS:          int(s.cfg.RetryMaxDelay / time.Millisecond),
		HTTP2Enabled:        !s.cfg.DisableHTTP2,
	}
	if !meta.RouteCachePersisted {
		meta.StatePath = ""
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(meta); err != nil {
		s.log.Error().Err(err).Msg("failed to encode meta response")
	}
}

func (s *Server) injectResponseHeaders(w http.ResponseWriter, bucketKey, route string, waited, planned, upstream time.Duration, hdr map[string]string) {
	if route != "" {
		w.Header().Set("X-Sirocco-Route", route)
	}
	if bucketKey != "" {
		w.Header().Set("X-Sirocco-Bucket-Key", bucketKey)
	}
	w.Header().Set("X-Sirocco-Waited", formatDurationSeconds(waited))
	w.Header().Set("X-Sirocco-Planned-Wait", formatDurationSeconds(planned))

	upstreamVal := formatDurationSeconds(upstream)
	if hdr != nil {
		if raw := hdr["x-sirocco-upstream-latency"]; raw != "" {
			upstreamVal = raw
		}
		if retries := hdr["x-sirocco-retries"]; retries != "" {
			w.Header().Set("X-Sirocco-Upstream-Retries", retries)
		}
		if status := hdr["x-sirocco-status"]; status != "" {
			w.Header().Set("X-Sirocco-Upstream-Status", status)
		}
	}
	w.Header().Set("X-Sirocco-Upstream-Latency", upstreamVal)
}

func formatDurationSeconds(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	return strconv.FormatFloat(d.Seconds(), 'f', 3, 64)
}

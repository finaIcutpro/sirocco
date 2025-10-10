package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	case "/_sirocco/dashboard":
		s.handleDashboard(w, r)
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
		var err error
		body, err = io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			s.log.Error().Err(err).Str("method", r.Method).Str("path", r.URL.Path).Msg("failed to read request body")
			http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	_, bucketKey, route := s.rl.Plan(r.Method, r.URL.Path, token)
	s.handleUpstream(w, r, bucketKey, token, body, route)
}

func (s *Server) handleUpstream(w http.ResponseWriter, r *http.Request, key, token string, body []byte, route string) {
	// rate limit acquire
	acquireStart := time.Now()
	release, plannedWait := s.rl.AcquireWithRoute(key, token, route, acquireStart)
	if plannedWait > 0 {
		s.log.Debug().Dur("wait", plannedWait).Str("key", key).Msg("rate limit wait")
		if err := waitWithContext(r.Context(), plannedWait); err != nil {
			release(false, nil)
			s.log.Debug().Err(err).Str("bucketKey", key).Str("route", route).Msg("request canceled while waiting for rate limit")
			return
		}
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
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
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
		RateLimitsAvoided   uint64  `json:"rate_limits_avoided"`
		RateLimitsHit       uint64  `json:"rate_limits_hit"`
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
		RateLimitsAvoided:   snap.RateLimitsAvoided,
		RateLimitsHit:       snap.RateLimitsHit,
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

type dashboardView struct {
	Title               string
	Generated           string
	Started             string
	Uptime              string
	UptimeSeconds       string
	RateLimitsAvoided   uint64
	RateLimitsHit       uint64
	AvoidanceRate       string
	HitRate             string
	Buckets             int
	Globals             int
	Routes              int
	InvalidEvents       int
	RouteCacheStatus    string
	RouteCachePersisted bool
	StatePath           string
	BotOverrides        int
	MaxUpstreamRetries  int
	RetryBase           string
	RetryMax            string
	HTTP2Status         string
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	snap := s.rl.Snapshot()
	uptime := time.Since(s.started)
	totalLimits := snap.RateLimitsAvoided + snap.RateLimitsHit
	avoidanceRate := "n/a"
	hitRate := "n/a"
	if totalLimits > 0 {
		avoidanceRate = fmt.Sprintf("%.1f%%", (float64(snap.RateLimitsAvoided)/float64(totalLimits))*100)
		hitRate = fmt.Sprintf("%.1f%%", (float64(snap.RateLimitsHit)/float64(totalLimits))*100)
	}
	view := dashboardView{
		Title:               "Sirocco Proxy Dashboard",
		Generated:           time.Now().Format(time.RFC1123),
		Started:             s.started.Format(time.RFC1123),
		Uptime:              humanDuration(uptime),
		UptimeSeconds:       fmt.Sprintf("%.0f", uptime.Seconds()),
		RateLimitsAvoided:   snap.RateLimitsAvoided,
		RateLimitsHit:       snap.RateLimitsHit,
		AvoidanceRate:       avoidanceRate,
		HitRate:             hitRate,
		Buckets:             snap.Buckets,
		Globals:             snap.Globals,
		Routes:              snap.Routes,
		InvalidEvents:       snap.InvalidEvents,
		RouteCacheStatus:    ternaryString(s.cfg.StatePath != "", "Persisted", "Disabled"),
		RouteCachePersisted: s.cfg.StatePath != "",
		StatePath:           s.cfg.StatePath,
		BotOverrides:        len(s.cfg.GlobalOverride),
		MaxUpstreamRetries:  s.cfg.MaxUpstreamRetries,
		RetryBase:           s.cfg.RetryBaseDelay.String(),
		RetryMax:            s.cfg.RetryMaxDelay.String(),
		HTTP2Status:         ternaryString(!s.cfg.DisableHTTP2, "Enabled", "Disabled"),
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	if err := dashboardTemplate.Execute(w, view); err != nil {
		s.log.Error().Err(err).Msg("failed to render dashboard")
	}
}

func ternaryString(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func humanDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	if d == 0 {
		return "0s"
	}
	var parts []string
	hours := d / time.Hour
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
		d -= hours * time.Hour
	}
	mins := d / time.Minute
	if mins > 0 {
		parts = append(parts, fmt.Sprintf("%dm", mins))
		d -= mins * time.Minute
	}
	secs := d / time.Second
	if secs > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", secs))
	}
	return strings.Join(parts, " ")
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

func waitWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

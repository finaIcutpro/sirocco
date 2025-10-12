package ratelimit

import (
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/melonly/sirocco/internal/config"
	"github.com/melonly/sirocco/internal/util"
)

// Manager coordinates global and route-scoped rate limits.
type Manager struct {
	cfg *config.Config
	log zerolog.Logger

	mu      sync.RWMutex
	buckets map[string]*routeLimiter
	globals map[string]*globalLimiter

	avoided atomic.Uint64
	hits    atomic.Uint64
	total   atomic.Uint64
	invalid atomic.Uint64
}

const (
	maxRetryAfter = 2 * time.Second
)

// Snapshot returns a summary of limiter state for diagnostics.
type Snapshot struct {
	Buckets           int    `json:"buckets"`
	Globals           int    `json:"globals"`
	InvalidEvents     int    `json:"invalid_events"`
	RateLimitsAvoided uint64 `json:"rate_limits_avoided"`
	RateLimitsHit     uint64 `json:"rate_limits_hit"`
	TotalRequests     uint64 `json:"total_requests"`
}

func NewManager(cfg *config.Config, log zerolog.Logger) *Manager {
	return &Manager{
		cfg:     cfg,
		log:     log,
		buckets: make(map[string]*routeLimiter),
		globals: make(map[string]*globalLimiter),
	}
}

// Plan returns the bucket key and normalized route pattern used for limiter coordination.
func (m *Manager) Plan(method, path, token string) (bucketKey, route string) {
	route = NormalizeRoute(method, path)
	if token == "" {
		return "r::" + route, route
	}
	tokenKey := tokenRouteKey(token)
	return "r:" + tokenKey + ":" + route, route
}

// AcquireWithRoute returns a release function and the amount of time the caller should wait
// before making the upstream request.
func (m *Manager) AcquireWithRoute(key, token, route string, want time.Time) (func(success bool, headers map[string]string), time.Duration) {
	m.total.Add(1)

	rl := m.getRouteLimiter(key, route)
	gl := m.getGlobalLimiter(token)

	wait := rl.reserve(want)
	if gl != nil {
		if gw := gl.reserve(want); gw > wait {
			wait = gw
		}
	}
	if wait > 0 {
		m.avoided.Add(1)
		m.log.Debug().Dur("wait", wait).Str("key", key).Str("token", util.MaskToken(token)).Str("route", route).Msg("rate limiting request")
	}

	release := func(success bool, headers map[string]string) {
		meta := parseReleaseMeta(headers)
		if meta.hasRetryAfter || meta.status == 429 {
			m.hits.Add(1)
		}
		if meta.isGlobal && gl != nil {
			gl.applyRetry(meta)
		} else {
			rl.apply(meta)
		}
		m.trackInvalid(route, meta)
	}

	return release, wait
}

func (m *Manager) getRouteLimiter(key, route string) *routeLimiter {
	m.mu.RLock()
	rl := m.buckets[key]
	m.mu.RUnlock()
	if rl != nil {
		return rl
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if rl = m.buckets[key]; rl != nil {
		return rl
	}
	rl = newRouteLimiter(route)
	m.buckets[key] = rl
	return rl
}

func (m *Manager) getGlobalLimiter(token string) *globalLimiter {
	if token == "" {
		return nil
	}
	m.mu.RLock()
	gl := m.globals[token]
	m.mu.RUnlock()
	if gl != nil {
		return gl
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if gl = m.globals[token]; gl != nil {
		return gl
	}
	base := 45
	if o, ok := m.cfg.GlobalOverride[token]; ok && o > 0 {
		base = o
	}
	gl = &globalLimiter{rate: base}
	m.globals[token] = gl
	return gl
}

func (m *Manager) trackInvalid(route string, meta releaseMeta) {
	if meta.status == 401 || meta.status == 403 || (meta.status == 429 && meta.scope != "shared") {
		m.invalid.Add(1)
		return
	}
	if meta.status == 404 && strings.HasPrefix(route, "POST /webhooks/:id/") {
		m.invalid.Add(1)
	}
}

// Snapshot returns a summary of limiter state for diagnostics.
func (m *Manager) Snapshot() Snapshot {
	snap := Snapshot{}
	m.mu.RLock()
	snap.Buckets = len(m.buckets)
	snap.Globals = len(m.globals)
	m.mu.RUnlock()
	snap.InvalidEvents = int(m.invalid.Load())
	snap.RateLimitsAvoided = m.avoided.Load()
	snap.RateLimitsHit = m.hits.Load()
	snap.TotalRequests = m.total.Load()
	return snap
}

// releaseMeta captures the signal headers returned by Discord.
type releaseMeta struct {
	scope         string
	bucketID      string
	limit         int
	remaining     int
	resetAfter    float64
	retryAfter    float64
	status        int
	isGlobal      bool
	hasLimit      bool
	hasRemaining  bool
	hasResetAfter bool
	hasRetryAfter bool
}

func parseReleaseMeta(headers map[string]string) releaseMeta {
	if headers == nil {
		return releaseMeta{}
	}
	scope := strings.ToLower(headers["x-ratelimit-scope"])
	globalHint := strings.ToLower(headers["x-ratelimit-global"]) == "true"

	meta := releaseMeta{
		scope:    scope,
		bucketID: headers["x-ratelimit-bucket"],
		status:   parseInt(headers["x-sirocco-status"]),
		isGlobal: scope == "global" || globalHint,
	}

	if limit, ok := parseIntOK(headers["x-ratelimit-limit"]); ok {
		meta.limit = limit
		meta.hasLimit = true
	}
	if remaining, ok := parseIntOK(headers["x-ratelimit-remaining"]); ok {
		meta.remaining = remaining
		meta.hasRemaining = true
	}
	if resetAfter, ok := parseFloatOK(headers["x-ratelimit-reset-after"]); ok {
		meta.resetAfter = resetAfter
		meta.hasResetAfter = true
	}
	if retryAfter, ok := parseFloatOK(headers["retry-after"]); ok {
		meta.retryAfter = retryAfter
		meta.hasRetryAfter = true
	}

	return meta
}

// NormalizeRoute returns Discord-style route pattern preserving major parameters.
func NormalizeRoute(method, path string) string {
	parts := strings.Split(path, "/")
	preserveNext := false
	for i := 1; i < len(parts); i++ {
		p := parts[i]
		low := strings.ToLower(p)
		switch low {
		case "channels", "guilds", "webhooks":
			preserveNext = true
		default:
			if preserveNext {
				preserveNext = false
			} else if isSnowflake(p) {
				parts[i] = ":id"
			}
		}
	}
	return method + " " + strings.Join(parts, "/")
}

func isSnowflake(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) >= 5
}

func tokenRouteKey(token string) string {
	sum := sha1.Sum([]byte(token))
	return hex.EncodeToString(sum[:])
}

// Helpers
func parseInt(s string) int { n, _ := strconv.Atoi(s); return n }

func parseIntOK(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseFloatOK(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func dur(sec float64) time.Duration {
	return time.Duration(sec * float64(time.Second))
}

func clampRetryAfterDuration(sec float64) time.Duration {
	if sec <= 0 {
		return 0
	}
	wait := dur(sec)
	if wait > maxRetryAfter {
		return maxRetryAfter
	}
	return wait
}

// routeLimiter enforces per-route limits based on heuristics and header feedback.
type routeLimiter struct {
	mu         sync.Mutex
	cap        int
	remaining  int
	window     time.Duration
	reset      time.Time
	blockUntil time.Time
}

func newRouteLimiter(route string) *routeLimiter {
	rl := &routeLimiter{}
	if capacity, windowSec, ok := Heuristic(route); ok {
		rl.cap = capacity
		rl.remaining = capacity
		rl.window = dur(windowSec)
	}
	return rl
}

func (rl *routeLimiter) reserve(now time.Time) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if now.Before(rl.blockUntil) {
		return rl.blockUntil.Sub(now)
	}
	if rl.window > 0 && (rl.reset.IsZero() || now.After(rl.reset)) {
		rl.reset = now.Add(rl.window)
		rl.remaining = rl.cap
	}
	if rl.cap == 0 {
		return 0
	}
	if rl.remaining > 0 {
		rl.remaining--
		return 0
	}
	wait := rl.reset.Sub(now)
	if wait < 0 {
		wait = 0
	}
	return wait
}

func (rl *routeLimiter) apply(meta releaseMeta) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	if meta.hasRetryAfter && meta.retryAfter > 0 {
		wait := clampRetryAfterDuration(meta.retryAfter)
		until := now.Add(wait)
		if until.After(rl.blockUntil) {
			rl.blockUntil = until
		}
	}
	if meta.hasLimit && meta.limit > 0 {
		rl.cap = meta.limit
		if !meta.hasRemaining {
			rl.remaining = meta.limit
		}
	}
	if meta.hasRemaining {
		rl.remaining = meta.remaining
		if rl.remaining < 0 {
			rl.remaining = 0
		}
	}
	if meta.hasResetAfter && meta.resetAfter > 0 {
		rl.window = dur(meta.resetAfter)
		rl.reset = now.Add(rl.window)
	} else if rl.window == 0 && rl.cap > 0 {
		rl.window = time.Second
		rl.reset = now.Add(rl.window)
	}
	if rl.cap > 0 && rl.remaining > rl.cap {
		rl.remaining = rl.cap
	}
}

// globalLimiter enforces per-token global limits.
type globalLimiter struct {
	mu         sync.Mutex
	rate       int
	used       int
	reset      time.Time
	blockUntil time.Time
}

func (gl *globalLimiter) reserve(now time.Time) time.Duration {
	gl.mu.Lock()
	defer gl.mu.Unlock()

	if now.Before(gl.blockUntil) {
		return gl.blockUntil.Sub(now)
	}
	if now.After(gl.reset) {
		gl.reset = now.Add(time.Second)
		gl.used = 0
	}
	if gl.rate <= 0 {
		return 0
	}
	if gl.used < gl.rate {
		gl.used++
		return 0
	}
	wait := gl.reset.Sub(now)
	if wait < 0 {
		wait = 0
	}
	return wait
}

func (gl *globalLimiter) applyRetry(meta releaseMeta) {
	if !meta.hasRetryAfter || meta.retryAfter <= 0 {
		return
	}
	gl.mu.Lock()
	defer gl.mu.Unlock()

	wait := clampRetryAfterDuration(meta.retryAfter)
	until := time.Now().Add(wait)
	if until.After(gl.blockUntil) {
		gl.blockUntil = until
	}
	if gl.rate > 1 {
		gl.rate--
	}
}

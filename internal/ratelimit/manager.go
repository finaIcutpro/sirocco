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

// Manager coordinates per-bucket and global limits
type Manager struct {
	cfg *config.Config
	log zerolog.Logger

	mu sync.RWMutex
	// key -> bucket limiter
	buckets map[string]*bucket
	// token -> global limiter
	globals map[string]*global

	guard   invalidGuard
	avoided atomic.Uint64
	hits    atomic.Uint64
	total   atomic.Uint64
}

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
	m := &Manager{
		cfg:     cfg,
		log:     log,
		buckets: make(map[string]*bucket),
		globals: make(map[string]*global),
		guard:   newInvalidGuard(),
	}
	return m
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

// AcquireWithRoute is like Acquire but also knows the route for header learning on commit
func (m *Manager) AcquireWithRoute(key, token, route string, want time.Time) (release func(success bool, headers map[string]string), wait time.Duration) {
	m.total.Add(1)
	g := m.getGlobal(token)
	b := m.getBucket(key)
	gpace := g.pace(want)
	gwait := g.when(want)
	if gpace > gwait {
		gwait = gpace
	}
	bwait := b.when(want)
	if bwait > gwait {
		gwait = bwait
	}
	gsoft := g.softDelay(want)
	if gsoft > gwait {
		gwait = gsoft
	}
	extra := m.guard.delay(want)
	if extra > gwait {
		gwait = extra
	}
	leave := b.enter()
	if gwait > 0 {
		m.avoided.Add(1)
		m.log.Debug().Dur("wait", gwait).Str("key", key).Str("token", util.MaskToken(token)).Str("route", route).Msg("rate limiting request")
	}
	return m.buildReleaseFunc(b, g, token, route, leave), gwait
}

func (m *Manager) buildReleaseFunc(b *bucket, g *global, token, route string, leave func()) func(bool, map[string]string) {
	return func(success bool, headers map[string]string) {
		meta := parseReleaseMeta(headers)
		m.updateLimiters(g, b, token, success, meta)
		m.markInvalidRequests(route, meta)
		leave()
	}
}

type releaseMeta struct {
	scope      string
	bucketID   string
	limit      int
	remaining  int
	resetAfter float64
	retryAfter float64
	status     int
	isGlobal   bool
}

func parseReleaseMeta(headers map[string]string) releaseMeta {
	if headers == nil {
		return releaseMeta{}
	}
	scope := strings.ToLower(headers["x-ratelimit-scope"])
	globalHint := strings.ToLower(headers["x-ratelimit-global"]) == "true"
	return releaseMeta{
		scope:      scope,
		bucketID:   headers["x-ratelimit-bucket"],
		limit:      parseInt(headers["x-ratelimit-limit"]),
		remaining:  parseInt(headers["x-ratelimit-remaining"]),
		resetAfter: parseFloatSeconds(headers["x-ratelimit-reset-after"]),
		retryAfter: parseFloatSeconds(headers["retry-after"]),
		status:     parseInt(headers["x-sirocco-status"]),
		isGlobal:   scope == "global" || globalHint,
	}
}

func (m *Manager) updateLimiters(g *global, b *bucket, token string, success bool, meta releaseMeta) {
	if meta.retryAfter > 0 || meta.status == 429 {
		m.hits.Add(1)
	}
	if meta.isGlobal {
		g.commitGlobal(meta.retryAfter)
		m.log.Debug().Str("token", util.MaskToken(token)).Float64("retryAfter", meta.retryAfter).Msg("global rate limit hit")
		return
	}
	b.commit(meta.limit, meta.remaining, meta.resetAfter, meta.retryAfter)
	if success {
		g.commitSuccess()
	}
}

func (m *Manager) markInvalidRequests(route string, meta releaseMeta) {
	if meta.status == 401 || meta.status == 403 || (meta.status == 429 && meta.scope != "shared") {
		m.guard.mark(time.Now())
		m.log.Debug().Int("status", meta.status).Str("scope", meta.scope).Msg("marked invalid request")
	}
	if meta.status == 404 && strings.HasPrefix(route, "POST /webhooks/:id/") {
		m.guard.mark(time.Now())
		m.log.Debug().Str("route", route).Msg("marked webhook 404")
	}
}

// Snapshot returns a summary of limiter state for diagnostics.
func (m *Manager) Snapshot() Snapshot {
	snap := Snapshot{}
	m.mu.RLock()
	snap.Buckets = len(m.buckets)
	snap.Globals = len(m.globals)
	m.mu.RUnlock()
	snap.InvalidEvents = m.guard.count(time.Now())
	snap.RateLimitsAvoided = m.avoided.Load()
	snap.RateLimitsHit = m.hits.Load()
	snap.TotalRequests = m.total.Load()
	return snap
}

// invalidGuard tracks recent invalid requests to avoid CF bans (10k/10min per IP)
type invalidGuard struct {
	mu        sync.Mutex
	events    []time.Time
	window    time.Duration
	threshold int
}

func newInvalidGuard() invalidGuard {
	return invalidGuard{window: 10 * time.Minute, threshold: 10000}
}

func (g *invalidGuard) mark(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events = append(g.events, now)
	g.compact(now)
}

func (g *invalidGuard) delay(now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.compact(now)
	n := len(g.events)
	if n == 0 {
		return 0
	}
	// start backing off at 85% of threshold, linearly up to 2s
	near := int(float64(g.threshold) * 0.85)
	if n <= near {
		return 0
	}
	ratio := float64(n-near) / float64(g.threshold-near)
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	return time.Duration(ratio * float64(2*time.Second))
}

func (g *invalidGuard) compact(now time.Time) {
	cutoff := now.Add(-g.window)
	i := 0
	for i < len(g.events) && g.events[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		copy(g.events, g.events[i:])
		g.events = g.events[:len(g.events)-i]
	}
}

func (g *invalidGuard) count(now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.compact(now)
	return len(g.events)
}

func (m *Manager) getBucket(key string) *bucket {
	m.mu.RLock()
	b := m.buckets[key]
	m.mu.RUnlock()
	if b != nil {
		return b
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if bb := m.buckets[key]; bb != nil {
		return bb
	}
	nb := &bucket{}
	m.buckets[key] = nb
	return nb
}

func (m *Manager) getGlobal(token string) *global {
	m.mu.RLock()
	g := m.globals[token]
	m.mu.RUnlock()
	if g != nil {
		return g
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if gg := m.globals[token]; gg != nil {
		return gg
	}
	base := 75 // ease default RPS to reduce premature throttling
	if o, ok := m.cfg.GlobalOverride[token]; ok && o > 0 {
		base = o
	}
	ng := &global{rps: base}
	m.globals[token] = ng
	return ng
}

// bucket models a per-route bucket with reset-after semantics
type bucket struct {
	mu        sync.Mutex
	reset     time.Time
	remaining int
	cap       int
	grace     int
	q         []chan struct{}
	confirmed bool
}

func (b *bucket) when(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.After(b.reset) {
		if b.cap > 0 {
			b.remaining = b.cap
			if b.grace == 0 {
				b.grace = 1
			}
		}
		return 0
	}
	if b.remaining > 0 {
		return 0
	}
	if b.grace > 0 && b.remaining > -b.grace {
		return 0
	}
	if now.Before(b.reset) {
		return b.reset.Sub(now)
	}
	return 0
}

func (b *bucket) commit(limit, remaining int, resetAfterSec, retryAfterSec float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	hasSignal := limit > 0 || resetAfterSec > 0 || retryAfterSec > 0
	targetGrace := 0
	if limit > 0 {
		b.cap = limit
		// allow a small negative grace to absorb burst jitter without instant blocking
		targetGrace = limit / 5
		if targetGrace < 1 {
			targetGrace = 1
		}
	}
	if retryAfterSec > 0 {
		b.reset = now.Add(dur(retryAfterSec))
		b.remaining = 0
		if targetGrace > 0 {
			b.grace = targetGrace
		} else if b.grace > 1 {
			b.grace = b.grace / 2
			if b.grace < 1 {
				b.grace = 1
			}
		}
		if hasSignal {
			b.confirmed = true
		}
		return
	}
	if resetAfterSec > 0 {
		b.reset = now.Add(dur(resetAfterSec))
		b.remaining = remaining
		if b.remaining < 0 {
			b.remaining = 0
		}
		if targetGrace > b.grace {
			b.grace = targetGrace
		}
		if b.cap > 0 && b.remaining > b.cap {
			b.remaining = b.cap
		}
		if hasSignal {
			b.confirmed = true
		}
		return
	}
	if hasSignal {
		b.confirmed = true
	}
}

// enter enforces strict FIFO for requests within a bucket
func (b *bucket) enter() (leave func()) {
	ch := make(chan struct{})
	b.mu.Lock()
	wait := len(b.q) > 0
	b.q = append(b.q, ch)
	// if there was someone ahead, we'll wait on our chan; else we're first and proceed
	b.mu.Unlock()
	if wait {
		<-ch
	}
	return func() {
		b.mu.Lock()
		if len(b.q) > 0 {
			// pop self
			b.q = b.q[1:]
			if len(b.q) > 0 {
				// signal next
				close(b.q[0])
			}
		}
		b.mu.Unlock()
	}
}

// global simple token bucket style based on RPS
type global struct {
	mu       sync.Mutex
	rps      int
	window   time.Time
	used     int
	succ     int
	lastTune time.Time
	next     time.Time
}

func (g *global) when(now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if now.After(g.window) {
		g.window = now.Add(time.Second)
		g.used = 0
		if now.After(g.next) {
			g.next = now
		}
	}
	if g.used < g.rps {
		return 0
	}
	return g.window.Sub(now)
}

// pace enforces a minimum spacing between requests based on the current RPS budget.
func (g *global) pace(now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.rps <= 0 {
		return 0
	}
	spacing := time.Second / time.Duration(g.rps)
	if spacing <= 0 {
		spacing = time.Millisecond
	}
	if now.Before(g.next) {
		wait := g.next.Sub(now)
		g.next = g.next.Add(spacing)
		return wait
	}
	g.next = now.Add(spacing)
	return 0
}

// commitGlobal applies Discord global retry-after signals and tunes the limiter.
func (g *global) commitGlobal(retryAfterSec float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if retryAfterSec > 0 {
		g.window = now.Add(dur(retryAfterSec))
		g.used = g.rps // block until window
		g.next = g.window
		// tune down softly
		if g.rps > 1 {
			dec := g.rps / 20
			if dec < 1 {
				dec = 1
			}
			g.rps -= dec
			if g.rps < 1 {
				g.rps = 1
			}
		}
	} else if now.After(g.window) {
		g.window = now.Add(time.Second)
		g.used = 0
		if now.After(g.next) {
			g.next = now
		}
	}
}

func (g *global) commitSuccess() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.used++
	g.succ++
	now := time.Now()
	if now.After(g.next) {
		g.next = now
	}
	if now.Sub(g.lastTune) > time.Second && g.succ >= g.rps {
		// gentle ramp up
		g.rps++
		g.succ = 0
		g.lastTune = now
	}
}

// softDelay nudges callers to back off before fully exhausting the global budget.
func (g *global) softDelay(now time.Time) time.Duration {
	g.mu.Lock()
	window := g.window
	used := g.used
	rps := g.rps
	g.mu.Unlock()
	if rps <= 0 {
		return 0
	}
	if now.After(window) || window.IsZero() {
		return 0
	}
	remaining := rps - used
	if remaining < 0 {
		remaining = 0
	}
	threshold := rps / 5
	if threshold < 1 {
		threshold = 1
	}
	if remaining > threshold {
		return 0
	}
	windowLeft := window.Sub(now)
	if windowLeft <= 0 {
		return 0
	}
	ratio := float64(threshold-remaining+1) / float64(threshold+1)
	wait := time.Duration(ratio * float64(windowLeft) * 0.1)
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	maxWait := wait + windowLeft/time.Duration(threshold+1)
	if maxWait <= wait {
		maxWait = wait + time.Millisecond
	}
	return util.JitterDuration(wait, maxWait)
}

// Helpers
func dur(sec float64) time.Duration { return time.Duration(sec * float64(time.Second)) }

func parseInt(s string) int { n, _ := strconv.Atoi(s); return n }
func parseFloatSeconds(s string) float64 {
	if s == "" {
		return 0
	}
	// can be integer or float seconds; if ms is provided elsewhere, it is typically retry-after header in seconds
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

// NormalizeRoute returns Discord-style route pattern preserving major parameters
func NormalizeRoute(method, path string) string {
	parts := strings.Split(path, "/")
	// parts[0] is ""
	// preserve next id after channels|guilds|webhooks
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
				// keep as-is
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
	// basic length heuristic (snowflakes are usually >= 17 digits)
	return len(s) >= 5
}

func tokenRouteKey(token string) string {
	if token == "" {
		return ""
	}
	sum := sha1.Sum([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (m *Manager) Close() {
	// No-op since state management was removed
}

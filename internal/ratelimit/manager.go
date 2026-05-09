package ratelimit

import (
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	GlobalRPS  int
	TokenRates map[string]int
}

type Manager struct {
	cfg Config
	log *slog.Logger

	mu      sync.RWMutex
	routes  map[string]*routeBucket
	globals map[string]*globalBucket

	requests atomic.Uint64
	waits    atomic.Uint64
	hits     atomic.Uint64
	invalid  atomic.Uint64
	dirty    atomic.Bool

	stateMu sync.RWMutex
	state   StateStatus
}

type Plan struct {
	Key      string
	Route    string
	TokenKey string
}

type Snapshot struct {
	Buckets           int         `json:"buckets"`
	Globals           int         `json:"globals"`
	InvalidEvents     uint64      `json:"invalid_events"`
	RateLimitsAvoided uint64      `json:"rate_limits_avoided"`
	RateLimitsHit     uint64      `json:"rate_limits_hit"`
	TotalRequests     uint64      `json:"total_requests"`
	State             StateStatus `json:"state"`
}

type ReleaseMeta struct {
	Headers map[string]string
	Status  int
}

func New(cfg Config, log *slog.Logger) *Manager {
	if cfg.GlobalRPS <= 0 {
		cfg.GlobalRPS = 45
	}
	if log == nil {
		log = slog.Default()
	}
	return &Manager{cfg: cfg, log: log, routes: make(map[string]*routeBucket), globals: make(map[string]*globalBucket)}
}

func (m *Manager) Plan(method, path, token string) Plan {
	route := Normalize(method, path)
	tokenKey := TokenKey(token)
	return Plan{Key: "r:" + tokenKey + ":" + route, Route: route, TokenKey: tokenKey}
}

func (m *Manager) Acquire(plan Plan, token string, now time.Time) (time.Duration, func(bool, ReleaseMeta)) {
	m.requests.Add(1)

	route := m.route(plan.Key, plan.Route)
	global := m.global(token)

	wait := route.reserve(now)
	if global != nil {
		if gw := global.reserve(now); gw > wait {
			wait = gw
		}
	}
	if wait > 0 {
		m.waits.Add(1)
	}

	release := func(success bool, meta ReleaseMeta) {
		parsed := parse(meta)
		if parsed.hasRetryAfter || parsed.status == 429 {
			m.hits.Add(1)
		}
		if parsed.global && global != nil {
			global.applyRetry(parsed)
		} else if route.apply(parsed) {
			m.dirty.Store(true)
		}
		m.trackInvalid(plan.Route, parsed)
	}

	return wait, release
}

func (m *Manager) Snapshot() Snapshot {
	m.mu.RLock()
	buckets := len(m.routes)
	globals := len(m.globals)
	m.mu.RUnlock()

	m.stateMu.RLock()
	state := m.state
	m.stateMu.RUnlock()

	return Snapshot{Buckets: buckets, Globals: globals, InvalidEvents: m.invalid.Load(), RateLimitsAvoided: m.waits.Load(), RateLimitsHit: m.hits.Load(), TotalRequests: m.requests.Load(), State: state}
}

func (m *Manager) Dirty() bool { return m.dirty.Load() }

func (m *Manager) route(key, route string) *routeBucket {
	m.mu.RLock()
	b := m.routes[key]
	m.mu.RUnlock()
	if b != nil {
		return b
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if b = m.routes[key]; b != nil {
		return b
	}
	b = newRouteBucket(route)
	m.routes[key] = b
	return b
}

func (m *Manager) global(token string) *globalBucket {
	if token == "" {
		return nil
	}
	m.mu.RLock()
	b := m.globals[token]
	m.mu.RUnlock()
	if b != nil {
		return b
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if b = m.globals[token]; b != nil {
		return b
	}
	rate := m.cfg.GlobalRPS
	if override := m.cfg.TokenRates[token]; override > 0 {
		rate = override
	}
	b = &globalBucket{rate: rate}
	m.globals[token] = b
	return b
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

type releaseMeta struct {
	scope         string
	bucketID      string
	limit         int
	remaining     int
	resetAfter    float64
	retryAfter    float64
	status        int
	global        bool
	hasLimit      bool
	hasRemaining  bool
	hasResetAfter bool
	hasRetryAfter bool
}

func parse(meta ReleaseMeta) releaseMeta {
	headers := meta.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	scope := strings.ToLower(headers["x-ratelimit-scope"])
	parsed := releaseMeta{scope: scope, bucketID: headers["x-ratelimit-bucket"], status: meta.Status, global: scope == "global" || strings.EqualFold(headers["x-ratelimit-global"], "true")}
	if parsed.status == 0 {
		parsed.status = atoi(headers["x-sirocco-status"])
	}
	parsed.limit, parsed.hasLimit = atoiOK(headers["x-ratelimit-limit"])
	parsed.remaining, parsed.hasRemaining = atoiOK(headers["x-ratelimit-remaining"])
	parsed.resetAfter, parsed.hasResetAfter = atofOK(headers["x-ratelimit-reset-after"])
	parsed.retryAfter, parsed.hasRetryAfter = atofOK(headers["retry-after"])
	return parsed
}

func atoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func atoiOK(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.Atoi(s)
	return v, err == nil
}

func atofOK(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	return v, err == nil
}

func durationSeconds(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

func retryAfter(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	d := durationSeconds(seconds)
	if d > 15*time.Minute {
		return 15 * time.Minute
	}
	return d
}

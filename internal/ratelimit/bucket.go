package ratelimit

import (
	"sync"
	"time"
)

type routeBucket struct {
	mu         sync.Mutex
	route      string
	bucketID   string
	cap        int
	remaining  int
	window     time.Duration
	reset      time.Time
	blockUntil time.Time
	updated    time.Time
}

func newRouteBucket(route string) *routeBucket {
	b := &routeBucket{route: route}
	if cap, windowSeconds, ok := Heuristic(route); ok {
		b.cap = cap
		b.remaining = cap
		b.window = durationSeconds(windowSeconds)
	}
	return b
}

func (b *routeBucket) reserve(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if now.Before(b.blockUntil) {
		return b.blockUntil.Sub(now)
	}
	if b.cap <= 0 || b.window <= 0 {
		return 0
	}
	if b.reset.IsZero() || !now.Before(b.reset) {
		b.reset = now.Add(b.window)
		b.remaining = b.cap
	}
	if b.remaining > 0 {
		b.remaining--
		return 0
	}

	overflow := -b.remaining
	b.remaining--
	windows := overflow / b.cap
	wait := b.reset.Add(time.Duration(windows) * b.window).Sub(now)
	if wait < 0 {
		return 0
	}
	return wait
}

func (b *routeBucket) apply(meta releaseMeta) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	changed := false
	now := time.Now()
	if meta.bucketID != "" && meta.bucketID != b.bucketID {
		b.bucketID = meta.bucketID
		changed = true
	}
	if meta.hasRetryAfter {
		until := now.Add(retryAfter(meta.retryAfter))
		if until.After(b.blockUntil) {
			b.blockUntil = until
			changed = true
		}
	}
	if meta.hasLimit && meta.limit > 0 && meta.limit != b.cap {
		b.cap = meta.limit
		changed = true
	}
	if meta.hasRemaining {
		if meta.remaining < 0 {
			meta.remaining = 0
		}
		if meta.remaining != b.remaining {
			b.remaining = meta.remaining
			changed = true
		}
	}
	if meta.hasResetAfter && meta.resetAfter > 0 {
		window := durationSeconds(meta.resetAfter)
		reset := now.Add(window)
		if window != b.window || !reset.Equal(b.reset) {
			b.window = window
			b.reset = reset
			changed = true
		}
	}
	if b.cap > 0 && b.remaining > b.cap {
		b.remaining = b.cap
		changed = true
	}
	if changed {
		b.updated = now
	}
	return changed
}

type globalBucket struct {
	mu         sync.Mutex
	rate       int
	used       int
	reset      time.Time
	blockUntil time.Time
}

func (b *globalBucket) reserve(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if now.Before(b.blockUntil) {
		return b.blockUntil.Sub(now)
	}
	if b.rate <= 0 {
		return 0
	}
	if b.reset.IsZero() || !now.Before(b.reset) {
		b.reset = now.Add(time.Second)
		b.used = 0
	}
	if b.used < b.rate {
		b.used++
		return 0
	}

	overflow := b.used - b.rate
	b.used++
	wait := b.reset.Add(time.Duration(overflow/b.rate) * time.Second).Sub(now)
	if wait < 0 {
		return 0
	}
	return wait
}

func (b *globalBucket) applyRetry(meta releaseMeta) {
	if !meta.hasRetryAfter || meta.retryAfter <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	until := time.Now().Add(retryAfter(meta.retryAfter))
	if until.After(b.blockUntil) {
		b.blockUntil = until
	}
}

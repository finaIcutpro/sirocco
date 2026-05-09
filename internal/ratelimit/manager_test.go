package ratelimit

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRouteBucketQueuesOverflow(t *testing.T) {
	m := New(Config{GlobalRPS: 100}, nil)
	plan := m.Plan("POST", "/channels/123456/messages", "token")
	now := time.Unix(100, 0)

	for i := 0; i < 5; i++ {
		wait, _ := m.Acquire(plan, "token", now)
		if wait != 0 {
			t.Fatalf("wait %d = %s, want 0", i, wait)
		}
	}
	wait, _ := m.Acquire(plan, "token", now)
	if wait <= 0 {
		t.Fatalf("overflow wait = %s, want positive", wait)
	}
}

func TestStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.json")
	m := New(Config{GlobalRPS: 45}, nil)
	plan := m.Plan("GET", "/channels/123456/messages", "token")
	_, release := m.Acquire(plan, "token", time.Now())
	release(true, ReleaseMeta{Status: 200, Headers: map[string]string{
		"x-ratelimit-bucket":      "bucket-a",
		"x-ratelimit-limit":       "9",
		"x-ratelimit-remaining":   "8",
		"x-ratelimit-reset-after": "1.5",
	}})

	if err := m.SaveState(path); err != nil {
		t.Fatal(err)
	}

	reloaded := New(Config{GlobalRPS: 45}, nil)
	if err := reloaded.LoadState(path, time.Hour); err != nil {
		t.Fatal(err)
	}
	if got := reloaded.Snapshot().State.Buckets; got != 1 {
		t.Fatalf("loaded buckets = %d, want 1", got)
	}
}

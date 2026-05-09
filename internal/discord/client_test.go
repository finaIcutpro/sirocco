package discord

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/melonly/sirocco/internal/config"
)

func TestClientRetriesIdempotentStatus(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()

	base, _ := url.Parse(ts.URL)
	client := NewClientWithHTTP(base, ts.Client(), config.HTTPConfig{RetryLimit: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}, nil)
	req := httptest.NewRequest(http.MethodGet, "http://proxy.test/api/v10/users/@me", nil)
	resp, meta, err := client.Do(context.Background(), req, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if string(data) != "ok" || meta.Retries != 1 || hits.Load() != 2 {
		t.Fatalf("body=%q retries=%d hits=%d", data, meta.Retries, hits.Load())
	}
}

func TestClientDoesNotRetryPost(t *testing.T) {
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	base, _ := url.Parse(ts.URL)
	client := NewClientWithHTTP(base, ts.Client(), config.HTTPConfig{RetryLimit: 3, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}, nil)
	req := httptest.NewRequest(http.MethodPost, "http://proxy.test/api/v10/channels/1/messages", nil)
	resp, _, err := client.Do(context.Background(), req, []byte(`{"content":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}
}

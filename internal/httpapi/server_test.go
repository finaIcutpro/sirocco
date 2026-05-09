package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/melonly/sirocco/internal/config"
	"github.com/melonly/sirocco/internal/discord"
	"github.com/melonly/sirocco/internal/ratelimit"
)

type fakeUpstream struct{}

func (fakeUpstream) Do(ctx context.Context, r *http.Request, body []byte) (*http.Response, discord.Meta, error) {
	resp := &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("proxied"))}
	resp.Header.Set("Content-Type", "text/plain")
	return resp, discord.Meta{Status: http.StatusCreated, Headers: map[string]string{"x-ratelimit-limit": "10", "x-ratelimit-remaining": "9", "x-ratelimit-reset-after": "1"}, Latency: time.Millisecond}, nil
}

func TestHealthAndProxy(t *testing.T) {
	cfg := config.Config{ListenAddress: "127.0.0.1:0", DiscordBaseURL: "https://discord.com", MaxBodyBytes: 1024, HTTP: config.HTTPConfig{RetryLimit: 1, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}, Rate: config.RateConfig{GlobalRPS: 45}}
	limiter := ratelimit.New(ratelimit.Config{GlobalRPS: 45}, nil)
	server := New(cfg, nil, limiter, fakeUpstream{}, nil)

	health := httptest.NewRecorder()
	server.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/_sirocco/health", nil))
	if health.Code != http.StatusOK || strings.TrimSpace(health.Body.String()) != "ok" {
		t.Fatalf("health code=%d body=%q", health.Code, health.Body.String())
	}

	proxy := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v10/channels/123/messages", strings.NewReader(`{"content":"hi"}`))
	req.Header.Set("Authorization", "Bot token")
	server.ServeHTTP(proxy, req)
	if proxy.Code != http.StatusCreated {
		t.Fatalf("proxy code = %d body=%q", proxy.Code, proxy.Body.String())
	}
	if proxy.Header().Get("X-Sirocco-Route") == "" || proxy.Header().Get("X-Sirocco-Upstream-Status") != "201" {
		t.Fatalf("missing sirocco headers: %#v", proxy.Header())
	}
}

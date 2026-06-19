package discord

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/melonly/sirocco/internal/config"
)

type Client struct {
	base       *url.URL
	http       *http.Client
	log        *slog.Logger
	retryLimit int
	retryBase  time.Duration
	retryMax   time.Duration
	requests   atomic.Uint64
	durationNs atomic.Uint64
	maxLatency atomic.Uint64
}

type Stats struct {
	TotalRequests        uint64  `json:"total_requests"`
	TotalDurationSeconds float64 `json:"total_duration_seconds"`
	AvgDurationSeconds   float64 `json:"avg_duration_seconds"`
	MaxDurationSeconds   float64 `json:"max_duration_seconds"`
}

type Meta struct {
	Headers map[string]string
	Status  int
	Retries int
	Latency time.Duration
}

func NewClient(baseURL string, cfg config.HTTPConfig, log *slog.Logger) (*Client, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}
	if cfg.OutboundIP != nil {
		dialer.LocalAddr = &net.TCPAddr{IP: cfg.OutboundIP}
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, ClientSessionCache: tls.NewLRUClientSessionCache(256)},
		ForceAttemptHTTP2:     !cfg.DisableHTTP2,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   512,
		MaxConnsPerHost:       1024,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.DialTimeout,
		ResponseHeaderTimeout: cfg.RequestTimeout,
		ExpectContinueTimeout: 250 * time.Millisecond,
	}

	return NewClientWithHTTP(base, &http.Client{Transport: transport, Timeout: cfg.RequestTimeout}, cfg, log), nil
}

func NewClientWithHTTP(base *url.URL, hc *http.Client, cfg config.HTTPConfig, log *slog.Logger) *Client {
	retryBase := cfg.RetryBaseDelay
	retryMax := cfg.RetryMaxDelay
	if retryMax < retryBase {
		retryMax = retryBase
	}
	if retryMax > 2*time.Second {
		retryMax = 2 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &Client{base: base, http: hc, log: log, retryLimit: cfg.RetryLimit, retryBase: retryBase, retryMax: retryMax}
}

func (c *Client) Do(ctx context.Context, in *http.Request, body []byte) (*http.Response, Meta, error) {
	var meta Meta

	for attempt := 0; ; attempt++ {
		upstream := c.upstreamURL(in.URL)
		req, err := c.request(ctx, in, upstream, body)
		if err != nil {
			return nil, meta, err
		}

		started := time.Now()
		resp, err := c.http.Do(req)
		latency := time.Since(started)
		c.recordLatency(latency)

		if err != nil {
			if !c.retryError(in.Method, err, attempt) {
				return nil, meta, err
			}
			if err := sleep(ctx, c.backoff(attempt+1)); err != nil {
				return nil, meta, err
			}
			continue
		}

		meta = Meta{Headers: capture(resp.Header), Status: resp.StatusCode, Retries: attempt, Latency: latency}
		meta.Headers["x-sirocco-status"] = strconv.Itoa(resp.StatusCode)

		if c.retryStatus(in.Method, resp.StatusCode, attempt) {
			_ = resp.Body.Close()
			if err := sleep(ctx, c.backoff(attempt+1)); err != nil {
				return nil, meta, err
			}
			continue
		}

		return resp, meta, nil
	}
}

func (c *Client) Stats() Stats {
	requests := c.requests.Load()
	totalNs := c.durationNs.Load()
	maxNs := c.maxLatency.Load()
	var avg float64
	if requests > 0 {
		avg = float64(totalNs) / float64(requests) / float64(time.Second)
	}
	return Stats{
		TotalRequests:        requests,
		TotalDurationSeconds: float64(totalNs) / float64(time.Second),
		AvgDurationSeconds:   avg,
		MaxDurationSeconds:   float64(maxNs) / float64(time.Second),
	}
}

func (c *Client) recordLatency(d time.Duration) {
	if d < 0 {
		return
	}
	ns := uint64(d)
	c.requests.Add(1)
	c.durationNs.Add(ns)
	for {
		cur := c.maxLatency.Load()
		if ns <= cur || c.maxLatency.CompareAndSwap(cur, ns) {
			return
		}
	}
}

func (c *Client) upstreamURL(in *url.URL) string {
	u := *in
	u.Scheme = c.base.Scheme
	u.Host = c.base.Host
	return u.String()
}

func (c *Client) request(ctx context.Context, in *http.Request, upstream string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, in.Method, upstream, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	copyHeaders(req.Header, in.Header)
	req.Header.Set("X-RateLimit-Precision", "millisecond")
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "sirocco (+https://github.com/melonly/sirocco)")
	}
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return req, nil
}

func (c *Client) retryError(method string, err error, attempt int) bool {
	if attempt >= c.retryLimit || !idempotent(method) || err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

func (c *Client) retryStatus(method string, status int, attempt int) bool {
	if attempt >= c.retryLimit || !idempotent(method) {
		return false
	}
	switch status {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout, 522, 524, 599:
		return true
	default:
		return false
	}
}

func (c *Client) backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	wait := c.retryBase * time.Duration(1<<uint(attempt-1))
	if wait > c.retryMax {
		wait = c.retryMax
	}
	low := wait / 2
	if low <= 0 {
		return wait
	}
	return low + time.Duration(rand.Int64N(int64(wait-low)))
}

func idempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions, http.MethodPatch:
		return true
	default:
		return false
	}
}

func capture(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for key, values := range h {
		if len(values) > 0 {
			out[strings.ToLower(key)] = values[0]
		}
	}
	return out
}

func copyHeaders(dst, src http.Header) {
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

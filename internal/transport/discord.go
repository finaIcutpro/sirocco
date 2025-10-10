package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/melonly/sirocco/internal/config"
	"github.com/melonly/sirocco/internal/util"
	"github.com/rs/zerolog"
)

type DiscordClient struct {
	base *url.URL
	hc   *http.Client
	log  zerolog.Logger

	maxRetries int
	retryBase  time.Duration
	retryMax   time.Duration
}

func NewDiscordClient(cfg *config.Config, log zerolog.Logger) (*DiscordClient, error) {
	bu, err := url.Parse(cfg.DiscordBaseURL)
	if err != nil {
		return nil, err
	}

	d := &net.Dialer{Timeout: cfg.DialTimeout}
	if cfg.OutboundIP != nil {
		d.LocalAddr = &net.TCPAddr{IP: cfg.OutboundIP}
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           d.DialContext,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ForceAttemptHTTP2:     !cfg.DisableHTTP2,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   512,
		MaxConnsPerHost:       512,
		TLSHandshakeTimeout:   cfg.DialTimeout,
		ExpectContinueTimeout: 500 * time.Millisecond,
		ResponseHeaderTimeout: cfg.RequestTimeout,
	}
	retryBase := cfg.RetryBaseDelay
	if retryBase <= 0 {
		retryBase = 200 * time.Millisecond
	}
	retryMax := cfg.RetryMaxDelay
	if retryMax <= retryBase {
		retryMax = retryBase * 4
	}
	maxRetries := cfg.MaxUpstreamRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	return &DiscordClient{
		base:       bu,
		hc:         &http.Client{Transport: tr, Timeout: cfg.RequestTimeout},
		log:        log,
		maxRetries: maxRetries,
		retryBase:  retryBase,
		retryMax:   retryMax,
	}, nil
}

// Do clones the incoming request, rewrites host to Discord base, performs it and returns the response and headers parsed for RL updates
func (d *DiscordClient) Do(ctx context.Context, in *http.Request, body []byte) (*http.Response, map[string]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	attempts := 0
	for {
		upstreamURL := d.rewriteURL(in.URL)
		req, err := d.buildRequest(ctx, in, &upstreamURL, body)
		if err != nil {
			d.log.Error().Err(err).Str("method", in.Method).Str("url", upstreamURL.String()).Msg("failed to create upstream request")
			return nil, nil, err
		}
		upstreamStr := upstreamURL.String()
		d.log.Debug().Str("method", in.Method).Str("url", upstreamStr).Int("attempt", attempts).Msg("sending upstream request")
		upstreamStart := time.Now()
		resp, err := d.hc.Do(req)
		if err != nil {
			if !d.shouldRetryError(in.Method, err, attempts) {
				d.log.Error().Err(err).Str("method", in.Method).Str("url", upstreamStr).Msg("upstream request failed")
				return nil, nil, err
			}
			attempts++
			backoff := d.nextBackoff(attempts)
			d.log.Warn().Err(err).Str("method", in.Method).Str("url", upstreamStr).Int("attempt", attempts).Dur("backoff", backoff).Msg("retrying after upstream error")
			if err := util.WaitContext(ctx, backoff); err != nil {
				return nil, nil, err
			}
			continue
		}
		latency := time.Since(upstreamStart)
		hdr := captureHeaders(resp.Header)
		hdr["x-sirocco-status"] = strconv.Itoa(resp.StatusCode)
		hdr["x-sirocco-upstream-latency"] = strconv.FormatFloat(latency.Seconds(), 'f', 3, 64)
		if attempts > 0 {
			hdr["x-sirocco-retries"] = strconv.Itoa(attempts)
		}
		if d.shouldRetryStatus(in.Method, resp.StatusCode, attempts) {
			resp.Body.Close()
			attempts++
			backoff := d.nextBackoff(attempts)
			d.log.Warn().Int("status", resp.StatusCode).Str("method", in.Method).Str("url", upstreamStr).Int("attempt", attempts).Dur("backoff", backoff).Msg("retrying after upstream status")
			if err := util.WaitContext(ctx, backoff); err != nil {
				return nil, nil, err
			}
			continue
		}
		d.log.Debug().Int("status", resp.StatusCode).Str("method", in.Method).Str("url", upstreamStr).Dur("latency", latency).Int("retries", attempts).Msg("upstream response")
		return resp, hdr, nil
	}
}

func (d *DiscordClient) rewriteURL(in *url.URL) url.URL {
	if in == nil {
		return *d.base
	}
	u := *in
	u.Scheme = d.base.Scheme
	u.Host = d.base.Host
	return u
}

func (d *DiscordClient) buildRequest(ctx context.Context, in *http.Request, upstreamURL *url.URL, body []byte) (*http.Request, error) {
	var reader io.ReadCloser
	if body != nil {
		reader = io.NopCloser(bytes.NewReader(body))
	} else if in.Body != nil {
		reader = in.Body
	}
	req, err := http.NewRequestWithContext(ctx, in.Method, upstreamURL.String(), reader)
	if err != nil {
		return nil, err
	}
	util.CopyHeaders(req.Header, in.Header)
	req.Header.Set("X-RateLimit-Precision", "millisecond")
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "sirocco-proxy (+https://github.com/melonly/sirocco)")
	}
	if body != nil {
		req.ContentLength = int64(len(body))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(body)), nil
		}
	}
	req.Host = upstreamURL.Host
	return req, nil
}

func (d *DiscordClient) shouldRetryError(method string, err error, attempts int) bool {
	if attempts >= d.maxRetries {
		return false
	}
	if !isIdempotent(method) {
		return false
	}
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() || netErr.Temporary() {
			return true
		}
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return true
		}
	}
	return false
}

func (d *DiscordClient) shouldRetryStatus(method string, status int, attempts int) bool {
	if attempts >= d.maxRetries {
		return false
	}
	if !isIdempotent(method) {
		return false
	}
	switch status {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	case 522, 524, 599: // cloudflare/op edge specific timeouts
		return true
	default:
		return false
	}
}

func (d *DiscordClient) nextBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}
	wait := d.retryBase * time.Duration(1<<uint(attempt-1))
	if wait < d.retryBase {
		wait = d.retryBase
	}
	if wait > d.retryMax {
		wait = d.retryMax
	}
	minBackoff := wait / 2
	if minBackoff <= 0 {
		minBackoff = wait
	}
	return util.JitterDuration(minBackoff, wait)
}

func captureHeaders(h http.Header) map[string]string {
	hdr := make(map[string]string, len(h))
	for k, vals := range h {
		if len(vals) > 0 {
			hdr[strings.ToLower(k)] = vals[0]
		}
	}
	return hdr
}

func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions, http.MethodTrace, http.MethodPatch:
		return true
	default:
		return false
	}
}

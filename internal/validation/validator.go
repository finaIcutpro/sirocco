package validation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	discordapi "github.com/melonly/sirocco/api/discord"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
)

type Validator struct {
	log     *slog.Logger
	router  routers.Router
	options *openapi3filter.Options

	total   atomic.Uint64
	blocked atomic.Uint64
	reasons reasonCounter
}

type Error struct {
	Status  int      `json:"-"`
	Reason  string   `json:"reason"`
	Message string   `json:"message"`
	Details []string `json:"details,omitempty"`
}

type Stats struct {
	TotalValidated uint64        `json:"total_validated"`
	TotalBlocked   uint64        `json:"total_blocked"`
	BlockRate      float64       `json:"block_rate"`
	Reasons        []ReasonCount `json:"reasons,omitempty"`
}

type ReasonCount struct {
	Reason string `json:"reason"`
	Count  uint64 `json:"count"`
}

func New(log *slog.Logger) (*Validator, error) {
	if log == nil {
		log = slog.Default()
	}
	if len(discordapi.OpenAPI) == 0 {
		return nil, errors.New("discord openapi spec is empty")
	}

	ctx := context.Background()
	loader := &openapi3.Loader{Context: ctx, IsExternalRefsAllowed: true}
	doc, err := loader.LoadFromData(discordapi.OpenAPI)
	if err != nil {
		return nil, fmt.Errorf("load discord openapi: %w", err)
	}
	normalize(doc)
	validationOpts := []openapi3.ValidationOption{openapi3.AllowExtraSiblingFields("identifier", "const", "contentEncoding", "contentMediaType")}
	if err := doc.Validate(ctx, validationOpts...); err != nil {
		return nil, fmt.Errorf("validate discord openapi: %w", err)
	}
	router, err := legacy.NewRouter(doc, validationOpts...)
	if err != nil {
		return nil, fmt.Errorf("build openapi router: %w", err)
	}

	return &Validator{log: log.With("component", "validation"), router: router, options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc, MultiError: true}}, nil
}

func (v *Validator) Validate(ctx context.Context, r *http.Request, body []byte) *Error {
	if r == nil {
		return nil
	}
	v.total.Add(1)

	if r.Method == http.MethodTrace {
		return v.block("method_not_allowed", http.StatusMethodNotAllowed, "method TRACE is not supported", nil)
	}

	req := cloneRequest(ctx, r, body)
	if req.Method == http.MethodHead {
		req.Method = http.MethodGet
	}

	if err := v.validate(ctx, req); err != nil {
		if stripped := stripDiscordAPIPrefix(req); stripped != nil {
			if retryErr := v.validate(ctx, stripped); retryErr != nil {
				return v.fromError(retryErr, r.Method, r.URL.Path)
			} else {
				return nil
			}
		}
		return v.fromError(err, r.Method, r.URL.Path)
	}
	return nil
}

func (v *Validator) Stats(limit int) Stats {
	total := v.total.Load()
	blocked := v.blocked.Load()
	reasons := v.reasons.snapshot(limit)
	var rate float64
	if total > 0 {
		rate = float64(blocked) / float64(total)
	}
	return Stats{TotalValidated: total, TotalBlocked: blocked, BlockRate: rate, Reasons: reasons}
}

func (e *Error) Payload() []byte {
	data, err := json.Marshal(e)
	if err != nil {
		return []byte(`{"message":"request blocked","reason":"request_invalid"}`)
	}
	return data
}

func (v *Validator) validate(ctx context.Context, req *http.Request) error {
	route, params, err := v.router.FindRoute(req)
	if err != nil {
		return err
	}
	return openapi3filter.ValidateRequest(ctx, &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route, Options: v.options})
}

func (v *Validator) fromError(err error, method, path string) *Error {
	var routeErr *routers.RouteError
	if errors.As(err, &routeErr) {
		reason := sanitize(routeErr.Reason)
		if reason == "" {
			reason = "route_not_matched"
		}
		status := http.StatusNotFound
		if strings.Contains(strings.ToLower(routeErr.Reason), "method") {
			status = http.StatusMethodNotAllowed
		}
		return v.block(reason, status, fmt.Sprintf("no discord route for %s %s", method, path), []string{routeErr.Error()})
	}

	var securityErr *openapi3filter.SecurityRequirementsError
	if errors.As(err, &securityErr) {
		return v.block("authentication_failed", http.StatusUnauthorized, "discord authentication requirements not satisfied", flatten(err))
	}

	var requestErr *openapi3filter.RequestError
	if errors.As(err, &requestErr) {
		reason := sanitize(requestErr.Reason)
		if reason == "" && requestErr.Err != nil {
			reason = sanitize(requestErr.Err.Error())
		}
		if reason == "" {
			reason = "request_invalid"
		}
		return v.block(reason, http.StatusBadRequest, "request blocked: "+reason, flatten(err))
	}

	reason := sanitize(err.Error())
	if reason == "" {
		reason = "request_invalid"
	}
	return v.block(reason, http.StatusBadRequest, "request blocked: "+reason, flatten(err))
}

func (v *Validator) block(reason string, status int, message string, details []string) *Error {
	if reason == "" {
		reason = "request_invalid"
	}
	v.blocked.Add(1)
	v.reasons.add(reason)
	v.log.Debug("blocked invalid request", "reason", reason, "status", status)
	return &Error{Status: status, Reason: reason, Message: message, Details: details}
}

func cloneRequest(ctx context.Context, r *http.Request, body []byte) *http.Request {
	clone := r.Clone(ctx)
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.ContentLength = int64(len(body))
	clone.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return clone
}

func stripDiscordAPIPrefix(req *http.Request) *http.Request {
	path := req.URL.Path
	if !strings.HasPrefix(path, "/api/v") {
		return nil
	}
	rest := strings.TrimPrefix(path, "/api/v")
	digits := 0
	for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
		digits++
	}
	if digits == 0 || digits == len(rest) || rest[digits] != '/' {
		return nil
	}
	clone := req.Clone(req.Context())
	urlCopy := *req.URL
	urlCopy.Path = rest[digits:]
	clone.URL = &urlCopy
	return clone
}

type reasonCounter struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func (c *reasonCounter) add(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = make(map[string]uint64)
	}
	c.counts[reason]++
}

func (c *reasonCounter) snapshot(limit int) []ReasonCount {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ReasonCount, 0, len(c.counts))
	for reason, count := range c.counts {
		out = append(out, ReasonCount{Reason: reason, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Reason < out[j].Reason
		}
		return out[i].Count > out[j].Count
	})
	if limit > 0 && len(out) > limit {
		return out[:limit]
	}
	return out
}

func flatten(err error) []string {
	if err == nil {
		return nil
	}
	var multi openapi3.MultiError
	if errors.As(err, &multi) {
		out := make([]string, 0, len(multi))
		for _, item := range multi {
			out = append(out, item.Error())
		}
		return dedupe(out)
	}
	return []string{err.Error()}
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, item := range in {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func sanitize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

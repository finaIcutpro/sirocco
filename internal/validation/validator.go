package validation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bytedance/sonic"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/rs/zerolog"
)

// Validator blocks discord-bound requests that violate the OpenAPI contract.
type Validator struct {
	log     zerolog.Logger
	router  routers.Router
	doc     *openapi3.T
	options *openapi3filter.Options

	totalValidated atomic.Uint64
	totalBlocked   atomic.Uint64
	reasons        *reasonCounter
}

// ReasonCount represents the frequency of a specific validation failure.
type ReasonCount struct {
	Reason string `json:"reason"`
	Count  uint64 `json:"count"`
}

// Stats captures an instantaneous view of validation outcomes.
type Stats struct {
	TotalValidated uint64        `json:"total_validated"`
	TotalBlocked   uint64        `json:"total_blocked"`
	BlockRate      float64       `json:"block_rate"`
	Reasons        []ReasonCount `json:"reasons"`
}

// Error describes a validation failure suitable for surfacing to callers.
type Error struct {
	Status  int
	Reason  string
	Message string
	Details []string
}

// ResponsePayload renders the error in the proxy's JSON envelope.
func (e *Error) ResponsePayload() []byte {
	type payload struct {
		Message string   `json:"message"`
		Reason  string   `json:"reason"`
		Details []string `json:"details,omitempty"`
	}
	resp := payload{
		Message: e.Message,
		Reason:  e.Reason,
		Details: e.Details,
	}
	data, err := sonic.Marshal(resp)
	if err != nil {
		// fall back to minimal payload if marshaling fails
		fallback := fmt.Sprintf(`{"message":"%s","reason":"%s"}`,
			jsonEscape(e.Message), jsonEscape(e.Reason))
		return []byte(fallback)
	}
	return data
}

// New builds a Validator backed by the embedded Discord OpenAPI document.
func New(log zerolog.Logger) (*Validator, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	loader := &openapi3.Loader{Context: ctx, IsExternalRefsAllowed: true}
	doc, err := loader.LoadFromData(discordOpenAPISpec)
	if err != nil {
		return nil, fmt.Errorf("load discord openapi: %w", err)
	}
	if err := doc.Validate(ctx); err != nil {
		return nil, fmt.Errorf("validate discord openapi: %w", err)
	}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		return nil, fmt.Errorf("build openapi router: %w", err)
	}
	options := &openapi3filter.Options{
		AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
		MultiError:         true,
		// Discord ignores writeOnly / readOnly mismatches in requests, so keep defaults.
	}
	return &Validator{
		log:     log.With().Str("component", "validation").Logger(),
		router:  router,
		doc:     doc,
		options: options,
		reasons: newReasonCounter(),
	}, nil
}

// Validate enforces the OpenAPI contract against the incoming request body.
func (v *Validator) Validate(ctx context.Context, r *http.Request, body []byte) *Error {
	if r == nil {
		return nil
	}
	v.totalValidated.Add(1)

	reqForValidation := r.Clone(ctx)
	reqForValidation.Body = io.NopCloser(bytes.NewReader(body))
	reqForValidation.ContentLength = int64(len(body))
	reqForValidation.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}

	methodForRouter := reqForValidation.Method
	if methodForRouter == http.MethodHead {
		methodForRouter = http.MethodGet
	}
	if methodForRouter == http.MethodTrace {
		// Discord does not expose TRACE routes; reject early.
		reason := "method_not_allowed"
		return v.block(reason, http.StatusMethodNotAllowed, fmt.Sprintf("method %s is not supported", r.Method), nil)
	}

	routerReq := reqForValidation.Clone(ctx)
	routerReq.Method = methodForRouter
	route, params, err := v.router.FindRoute(routerReq)
	if err != nil {
		return v.blockFromRouterError(err, r.Method, reqForValidation.URL.Path)
	}
	reqForValidation.Method = methodForRouter

	input := &openapi3filter.RequestValidationInput{
		Request:    reqForValidation,
		PathParams: params,
		Route:      route,
		Options:    v.options,
	}

	if err := openapi3filter.ValidateRequest(ctx, input); err != nil {
		return v.blockFromValidationError(err)
	}

	return nil
}

// Stats returns recent validation counters limited to the top N reasons.
func (v *Validator) Stats(limit int) Stats {
	validated := v.totalValidated.Load()
	blocked := v.totalBlocked.Load()
	reasons := v.reasons.snapshot()
	if limit > 0 && len(reasons) > limit {
		reasons = reasons[:limit]
	}
	var rate float64
	if validated > 0 {
		rate = float64(blocked) / float64(validated)
	}
	return Stats{
		TotalValidated: validated,
		TotalBlocked:   blocked,
		BlockRate:      rate,
		Reasons:        reasons,
	}
}

func (v *Validator) blockFromRouterError(err error, method, path string) *Error {
	msg := err.Error()
	reason := "route_not_matched"
	status := http.StatusNotFound

	var routeErr *routers.RouteError
	if errors.As(err, &routeErr) {
		reason = sanitizeReason(routeErr.Reason)
		switch strings.ToLower(routeErr.Reason) {
		case "method not allowed":
			status = http.StatusMethodNotAllowed
		case "path not found", "route not found":
			status = http.StatusNotFound
		default:
			status = http.StatusBadRequest
		}
	} else if strings.Contains(strings.ToLower(msg), "method") && strings.Contains(strings.ToLower(msg), "allow") {
		reason = "method_not_allowed"
		status = http.StatusMethodNotAllowed
	} else if strings.Contains(strings.ToLower(msg), "not found") {
		reason = "unknown_route"
		status = http.StatusNotFound
	}

	detail := fmt.Sprintf("no discord route for %s %s", method, path)
	return v.block(reason, status, detail, []string{msg})
}

func (v *Validator) blockFromValidationError(err error) *Error {
	var reqErr *openapi3filter.RequestError
	if errors.As(err, &reqErr) {
		reason := sanitizeReason(reqErr.Reason)
		if reason == "" && reqErr.Err != nil {
			reason = sanitizeReason(reqErr.Err.Error())
		}
		if reason == "" {
			reason = "request_invalid"
		}
		details := flattenErrors(reqErr.Err)
		if len(details) == 0 {
			details = []string{reqErr.Error()}
		}
		message := fmt.Sprintf("request blocked: %s", reason)
		return v.block(reason, http.StatusBadRequest, message, details)
	}

	var secErr *openapi3filter.SecurityRequirementsError
	if errors.As(err, &secErr) {
		reason := "authentication_failed"
		details := flattenErrors(secErr)
		if len(details) == 0 {
			details = []string{secErr.Error()}
		}
		message := "discord authentication requirements not satisfied"
		return v.block(reason, http.StatusUnauthorized, message, details)
	}

	reason := sanitizeReason(err.Error())
	if reason == "" {
		reason = "request_invalid"
	}
	details := flattenErrors(err)
	if len(details) == 0 {
		details = []string{err.Error()}
	}
	message := fmt.Sprintf("request blocked: %s", reason)
	return v.block(reason, http.StatusBadRequest, message, details)
}

func (v *Validator) block(reason string, status int, message string, details []string) *Error {
	v.totalBlocked.Add(1)
	if reason == "" {
		reason = "request_invalid"
	}
	v.reasons.add(reason)
	if len(details) == 0 {
		details = nil
	}
	v.log.Debug().Str("reason", reason).Int("status", status).Msg("blocked invalid request")
	return &Error{
		Status:  status,
		Reason:  reason,
		Message: message,
		Details: details,
	}
}

// reasonCounter tracks occurrences per reason string.
type reasonCounter struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func newReasonCounter() *reasonCounter {
	return &reasonCounter{counts: make(map[string]uint64)}
}

func (rc *reasonCounter) add(reason string) {
	if reason == "" {
		reason = "request_invalid"
	}
	rc.mu.Lock()
	rc.counts[reason]++
	rc.mu.Unlock()
}

func (rc *reasonCounter) snapshot() []ReasonCount {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	out := make([]ReasonCount, 0, len(rc.counts))
	for reason, count := range rc.counts {
		out = append(out, ReasonCount{Reason: reason, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Reason < out[j].Reason
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func flattenErrors(err error) []string {
	if err == nil {
		return nil
	}

	var list []string
	var multi openapi3.MultiError
	if errors.As(err, &multi) {
		for _, item := range multi {
			list = append(list, item.Error())
		}
		return list
	}

	var reqErr *openapi3filter.RequestError
	if errors.As(err, &reqErr) {
		inner := flattenErrors(reqErr.Err)
		if len(inner) > 0 {
			list = append(list, inner...)
		}
		if reqErr.Reason != "" {
			list = append(list, reqErr.Reason)
		}
		if len(list) == 0 {
			list = append(list, reqErr.Error())
		}
		return dedupeStrings(list)
	}

	return []string{err.Error()}
}

func dedupeStrings(in []string) []string {
	if len(in) <= 1 {
		return in
	}
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

func sanitizeReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return ""
	}
	reason = strings.ToLower(reason)
	replacements := map[string]string{
		" ": "_",
		"-": "_",
		"/": "_",
		":": "_",
		",": "",
		".": "",
		"(": "",
		")": "",
	}
	for old, newVal := range replacements {
		reason = strings.ReplaceAll(reason, old, newVal)
	}
	reason = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' {
			return r
		}
		if r >= '0' && r <= '9' {
			return r
		}
		if r == '_' {
			return r
		}
		return -1
	}, reason)
	if reason == "" {
		return "request_invalid"
	}
	return reason
}

func jsonEscape(value string) string {
	buf, err := sonic.Marshal(value)
	if err != nil || len(buf) < 2 {
		return value
	}
	return string(buf[1 : len(buf)-1])
}

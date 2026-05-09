package validation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidatorLoadsOfficialSpec(t *testing.T) {
	if _, err := New(nil); err != nil {
		t.Fatal(err)
	}
}

func TestValidatorAcceptsDiscordAPIPrefix(t *testing.T) {
	v, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v10/users/@me", nil)
	req.Header.Set("Authorization", "Bot test-token")
	if got := v.Validate(context.Background(), req, nil); got != nil {
		t.Fatalf("Validate() = %#v, want nil", got)
	}
}

func TestValidatorReportsStrippedRouteError(t *testing.T) {
	v, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v10/definitely-not-a-discord-route", nil)
	got := v.Validate(context.Background(), req, nil)
	if got == nil {
		t.Fatal("Validate() = nil, want route error")
	}
	if got.Message == "no discord route for GET /api/v10/users/@me" {
		t.Fatalf("unexpected stale route error: %#v", got)
	}
}

func TestValidatorAcceptsAutocompleteInteractionCallback(t *testing.T) {
	v, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}

	body := `{"data":{"choices":[{"name":"Default","value":"0"},{"name":"Texas Department of Public Safety","value":"7355293666295943168"}]},"type":8}`
	req := interactionCallbackRequest(body)
	if got := v.Validate(context.Background(), req, []byte(body)); got != nil {
		t.Fatalf("Validate() = %#v, want nil", got)
	}
}

func TestValidatorAcceptsDeferredUpdateInteractionCallback(t *testing.T) {
	v, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}

	body := `{"data":{"flags":64},"type":6}`
	req := interactionCallbackRequest(body)
	if got := v.Validate(context.Background(), req, []byte(body)); got != nil {
		t.Fatalf("Validate() = %#v, want nil", got)
	}
}

func interactionCallbackRequest(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/v10/interactions/123456789012345678/test-token/callback", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

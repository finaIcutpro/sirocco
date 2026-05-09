package ratelimit

import "testing"

func TestNormalizePreservesMajorParameters(t *testing.T) {
	got := Normalize("POST", "/api/v10/channels/123456/messages/987654")
	want := "POST /api/v10/channels/123456/messages/:id"
	if got != want {
		t.Fatalf("Normalize() = %q, want %q", got, want)
	}
}

func TestTokenKeyDoesNotExposeToken(t *testing.T) {
	token := "very-secret-token"
	key := TokenKey(token)
	if key == "" || key == token {
		t.Fatalf("bad token key %q", key)
	}
}

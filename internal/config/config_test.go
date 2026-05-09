package config

import (
	"testing"
	"time"
)

func TestLoadCompatibilityEnv(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("BIND_IP", "127.0.0.1")
	t.Setenv("DISCORD_BASE_URL", "https://example.test")
	t.Setenv("VALIDATION_ENABLED", "false")
	t.Setenv("REQUEST_TIMEOUT", "250")
	t.Setenv("RATELIMIT_OVERRIDES", "token-a:12,broken,token-b:nope")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddress != "127.0.0.1:9090" {
		t.Fatalf("listen address = %q", cfg.ListenAddress)
	}
	if cfg.DiscordBaseURL != "https://example.test" {
		t.Fatalf("discord base = %q", cfg.DiscordBaseURL)
	}
	if cfg.ValidationEnabled {
		t.Fatal("validation should be disabled")
	}
	if cfg.HTTP.RequestTimeout != 250*time.Millisecond {
		t.Fatalf("request timeout = %s", cfg.HTTP.RequestTimeout)
	}
	if cfg.Rate.TokenRates["token-a"] != 12 {
		t.Fatalf("token override not parsed: %#v", cfg.Rate.TokenRates)
	}
}

func TestValidateRejectsBadBaseURL(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.DiscordBaseURL = "://bad"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

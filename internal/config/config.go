package config

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	LogLevel          string
	BindAddr          string
	Port              int
	DiscordBaseURL    string // e.g. https://discord.com
	DisableHTTP2      bool
	ValidationEnabled bool

	StatePath string

	RequestTimeout  time.Duration
	DialTimeout     time.Duration
	IdleConnTimeout time.Duration

	MaxUpstreamRetries int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration

	OutboundIP net.IP // force local address for egress

	// rate limit
	GlobalOverride map[string]int // userID -> RPS
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getdurms(key string, def int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Millisecond
		}
	}
	return time.Duration(def) * time.Millisecond
}

func getbool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		v = strings.ToLower(strings.TrimSpace(v))
		switch v {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	return def
}

func Load() *Config {
	c := &Config{
		LogLevel:           getenv("LOG_LEVEL", "info"),
		BindAddr:           getenv("BIND_IP", "0.0.0.0"),
		Port:               getint("PORT", 8080),
		DiscordBaseURL:     getenv("DISCORD_BASE_URL", "https://discord.com"),
		DisableHTTP2:       getenv("DISABLE_HTTP_2", "false") == "true",
		ValidationEnabled:  getbool("VALIDATION_ENABLED", true),
		StatePath:          getenv("STATE_PATH", defaultStatePath()),
		RequestTimeout:     getdurms("REQUEST_TIMEOUT", 5000),
		DialTimeout:        getdurms("DIAL_TIMEOUT", 2500),
		IdleConnTimeout:    getdurms("IDLE_CONN_TIMEOUT", 90000),
		MaxUpstreamRetries: getint("UPSTREAM_RETRY_LIMIT", 3),
		RetryBaseDelay:     getdurms("UPSTREAM_RETRY_BASE_DELAY", 200),
		RetryMaxDelay:      getdurms("UPSTREAM_RETRY_MAX_DELAY", 2000),
		GlobalOverride:     parseOverrides(getenv("RATELIMIT_OVERRIDES", "")),
	}
	if ip := net.ParseIP(getenv("OUTBOUND_IP", "")); ip != nil {
		c.OutboundIP = ip
	}
	return c
}

func defaultStatePath() string {
	if dir, err := os.UserCacheDir(); err == nil && dir != "" {
		dir = filepath.Join(dir, "sirocco")
		return filepath.Join(dir, "route-state.json")
	}
	return "sirocco_state.json"
}

func parseOverrides(in string) map[string]int {
	m := make(map[string]int)
	if in == "" {
		return m
	}
	for _, part := range strings.Split(in, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.Split(part, ":")
		if len(kv) != 2 {
			continue
		}
		if n, err := strconv.Atoi(kv[1]); err == nil {
			m[kv[0]] = n
		}
	}
	return m
}

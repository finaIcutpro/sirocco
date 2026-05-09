package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddress     string
	DiscordBaseURL    string
	LogLevel          string
	ValidationEnabled bool
	StatePath         string
	StateMaxAge       time.Duration
	StateFlushEvery   time.Duration
	ShutdownTimeout   time.Duration
	MaxBodyBytes      int64

	HTTP HTTPConfig
	Rate RateConfig
}

type HTTPConfig struct {
	RequestTimeout  time.Duration
	DialTimeout     time.Duration
	IdleConnTimeout time.Duration
	DisableHTTP2    bool
	OutboundIP      net.IP
	RetryLimit      int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
}

type RateConfig struct {
	GlobalRPS  int
	TokenRates map[string]int
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddress:     listenAddress(),
		DiscordBaseURL:    getenv("SIROCCO_DISCORD_BASE_URL", getenv("DISCORD_BASE_URL", "https://discord.com")),
		LogLevel:          getenv("SIROCCO_LOG_LEVEL", getenv("LOG_LEVEL", "info")),
		ValidationEnabled: getbool("SIROCCO_VALIDATION_ENABLED", getbool("VALIDATION_ENABLED", true)),
		StatePath:         getenv("SIROCCO_STATE_PATH", getenv("STATE_PATH", defaultStatePath())),
		StateMaxAge:       getdur("SIROCCO_STATE_MAX_AGE", 24*time.Hour),
		StateFlushEvery:   getdur("SIROCCO_STATE_FLUSH_EVERY", 30*time.Second),
		ShutdownTimeout:   getdur("SIROCCO_SHUTDOWN_TIMEOUT", 5*time.Second),
		MaxBodyBytes:      getbytes("SIROCCO_MAX_BODY_BYTES", 32<<20),
		HTTP: HTTPConfig{
			RequestTimeout:  getdur("SIROCCO_REQUEST_TIMEOUT", getdur("REQUEST_TIMEOUT", 5*time.Second)),
			DialTimeout:     getdur("SIROCCO_DIAL_TIMEOUT", getdur("DIAL_TIMEOUT", 2500*time.Millisecond)),
			IdleConnTimeout: getdur("SIROCCO_IDLE_CONN_TIMEOUT", getdur("IDLE_CONN_TIMEOUT", 90*time.Second)),
			DisableHTTP2:    getbool("SIROCCO_DISABLE_HTTP2", getbool("DISABLE_HTTP_2", false)),
			RetryLimit:      getint("SIROCCO_UPSTREAM_RETRY_LIMIT", getint("UPSTREAM_RETRY_LIMIT", 3)),
			RetryBaseDelay:  getdur("SIROCCO_UPSTREAM_RETRY_BASE_DELAY", getdur("UPSTREAM_RETRY_BASE_DELAY", 200*time.Millisecond)),
			RetryMaxDelay:   getdur("SIROCCO_UPSTREAM_RETRY_MAX_DELAY", getdur("UPSTREAM_RETRY_MAX_DELAY", 2*time.Second)),
		},
		Rate: RateConfig{
			GlobalRPS:  getint("SIROCCO_GLOBAL_RPS", 45),
			TokenRates: parseRates(getenv("SIROCCO_TOKEN_RATES", getenv("RATELIMIT_OVERRIDES", ""))),
		},
	}

	if ip := net.ParseIP(getenv("SIROCCO_OUTBOUND_IP", getenv("OUTBOUND_IP", ""))); ip != nil {
		cfg.HTTP.OutboundIP = ip
	}

	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	var errs []error

	if strings.TrimSpace(c.ListenAddress) == "" {
		errs = append(errs, errors.New("listen address is required"))
	}

	u, err := url.Parse(c.DiscordBaseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		errs = append(errs, fmt.Errorf("discord base url must be absolute: %q", c.DiscordBaseURL))
	}

	if c.HTTP.RequestTimeout <= 0 {
		errs = append(errs, errors.New("request timeout must be positive"))
	}
	if c.HTTP.DialTimeout <= 0 {
		errs = append(errs, errors.New("dial timeout must be positive"))
	}
	if c.HTTP.RetryLimit < 0 {
		errs = append(errs, errors.New("retry limit cannot be negative"))
	}
	if c.HTTP.RetryBaseDelay <= 0 || c.HTTP.RetryMaxDelay <= 0 {
		errs = append(errs, errors.New("retry delays must be positive"))
	}
	if c.Rate.GlobalRPS <= 0 {
		errs = append(errs, errors.New("global rps must be positive"))
	}
	if c.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("max body bytes must be positive"))
	}

	return errors.Join(errs...)
}

func listenAddress() string {
	if v := os.Getenv("SIROCCO_LISTEN"); v != "" {
		return v
	}
	bind := getenv("BIND_IP", "0.0.0.0")
	port := getenv("PORT", "8080")
	return net.JoinHostPort(bind, port)
}

func getenv(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func getint(key string, fallback int) int {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil {
			return v
		}
	}
	return fallback
}

func getbytes(key string, fallback int64) int64 {
	if raw := strings.TrimSpace(os.Getenv(key)); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			return v
		}
	}
	return fallback
}

func getbool(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func getdur(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	if n, err := strconv.Atoi(raw); err == nil {
		return time.Duration(n) * time.Millisecond
	}
	return fallback
}

func parseRates(raw string) map[string]int {
	rates := make(map[string]int)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, val, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		rate, err := strconv.Atoi(strings.TrimSpace(val))
		if err != nil || rate <= 0 {
			continue
		}
		rates[strings.TrimSpace(key)] = rate
	}
	return rates
}

func defaultStatePath() string {
	dir, err := os.UserCacheDir()
	if err != nil || dir == "" {
		return "sirocco-state.json"
	}
	return filepath.Join(dir, "sirocco", "routes.json")
}

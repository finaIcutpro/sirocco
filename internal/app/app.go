package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/melonly/sirocco/internal/config"
	"github.com/melonly/sirocco/internal/discord"
	"github.com/melonly/sirocco/internal/httpapi"
	"github.com/melonly/sirocco/internal/ratelimit"
	"github.com/melonly/sirocco/internal/validation"
)

func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logger(cfg.LogLevel)

	limiter := ratelimit.New(ratelimit.Config{GlobalRPS: cfg.Rate.GlobalRPS, TokenRates: cfg.Rate.TokenRates}, log)
	if err := limiter.LoadState(cfg.StatePath, cfg.StateMaxAge); err != nil {
		log.Warn("route state was not loaded", "error", err, "path", cfg.StatePath)
	}

	client, err := discord.NewClient(cfg.DiscordBaseURL, cfg.HTTP, log)
	if err != nil {
		return fmt.Errorf("discord client: %w", err)
	}

	var validator httpapi.Validator
	if cfg.ValidationEnabled {
		validator, err = validation.New(log)
		if err != nil {
			return fmt.Errorf("validator: %w", err)
		}
	}

	server := httpapi.New(cfg, log, limiter, client, validator)
	errs := make(chan error, 1)
	go func() { errs <- server.Start() }()

	log.Info("sirocco listening", "addr", cfg.ListenAddress, "validation", cfg.ValidationEnabled, "state_path", cfg.StatePath)
	stopSaver := startStateSaver(ctx, log, limiter, cfg.StatePath, cfg.StateFlushEvery)

	select {
	case <-ctx.Done():
	case err := <-errs:
		stopSaver()
		return err
	}

	stopSaver()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := limiter.SaveState(cfg.StatePath); err != nil {
		log.Warn("route state was not saved", "error", err, "path", cfg.StatePath)
	}
	return nil
}

func startStateSaver(ctx context.Context, log *slog.Logger, limiter *ratelimit.Manager, path string, every time.Duration) func() {
	if path == "" || every <= 0 {
		return func() {}
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				if limiter.Dirty() {
					if err := limiter.SaveState(path); err != nil {
						log.Warn("route state save failed", "error", err, "path", path)
					}
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func logger(level string) *slog.Logger {
	var parsed slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		parsed = slog.LevelDebug
	case "warn", "warning":
		parsed = slog.LevelWarn
	case "error":
		parsed = slog.LevelError
	default:
		parsed = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parsed}))
}

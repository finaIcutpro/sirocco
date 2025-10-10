package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/melonly/sirocco/internal/config"
	"github.com/melonly/sirocco/internal/logging"
	"github.com/melonly/sirocco/internal/proxy"
	"github.com/melonly/sirocco/internal/ratelimit"
	"github.com/melonly/sirocco/internal/transport"
)

func main() {
	cfg := config.Load()
	log := logging.New(cfg)

	// transport to Discord
	dc, err := transport.NewDiscordClient(cfg, log)
	if err != nil {
		log.Fatal().Err(err).Msg("transport init failed")
	}

	// rate limiter
	rl := ratelimit.NewManager(cfg, log)
	defer rl.Close()

	// proxy server
	srv := proxy.NewServer(cfg, log, rl, dc)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatal().Err(err).Msg("proxy start failed")
		}
	}()

	// graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info().Msg("shutting down...")
	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_ = srv.Shutdown(shutdownCtx)
}

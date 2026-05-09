package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/melonly/sirocco/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "sirocco: %v\n", err)
		os.Exit(1)
	}
}

package logging

import (
	"os"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/melonly/sirocco/internal/config"
)

func New(cfg *config.Config) zerolog.Logger {
	level := zerolog.InfoLevel
	switch strings.ToLower(cfg.LogLevel) {
	case "trace":
		level = zerolog.TraceLevel
	case "debug":
		level = zerolog.DebugLevel
	case "info":
		level = zerolog.InfoLevel
	case "warn":
		level = zerolog.WarnLevel
	case "error":
		level = zerolog.ErrorLevel
	case "fatal":
		level = zerolog.FatalLevel
	case "panic":
		level = zerolog.PanicLevel
	}
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	l := log.Output(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.Out = os.Stdout
		w.TimeFormat = "15:04:05.000"
	})).Level(level)
	return l
}

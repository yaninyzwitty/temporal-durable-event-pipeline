package logger

import (
	"log/slog"
	"os"
)

type Env string

const (
	EnvDevelopment Env = "development"
	EnvProduction  Env = "production"
)

func New(env Env) *slog.Logger {
	var handler slog.Handler
	switch env {
	case EnvProduction:
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	case EnvDevelopment:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})
	default:
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})
	}
	return slog.New(handler)

}

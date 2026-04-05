package poller_test

import (
	"io"
	"log/slog"
)

func newSlogLoggerDiscard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{AddSource: false}))
}

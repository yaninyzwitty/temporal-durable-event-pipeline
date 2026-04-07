package poller

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Listener interface {
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
	Close(ctx context.Context) error
}

type PGListener struct {
	conn     *pgx.Conn
	channel  string
	logger   *slog.Logger
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewPGListener(conn *pgx.Conn, channel string, logger *slog.Logger) *PGListener {
	if logger == nil {
		logger = slog.Default()
	}
	if channel == "" {
		channel = "events_notification"
	}

	return &PGListener{
		conn:    conn,
		channel: channel,
		logger:  logger,
		stopCh:  make(chan struct{}),
	}
}

func (l *PGListener) Start(ctx context.Context) error {
	_, err := l.conn.Exec(ctx, "LISTEN "+l.channel)
	if err != nil {
		return err
	}
	l.logger.Info("started listening on channel", "channel", l.channel)
	return nil
}

func (l *PGListener) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.stopCh:
		return nil, nil
	default:
	}

	notif, err := l.conn.WaitForNotification(ctx)
	if err != nil {
		return nil, err
	}
	return notif, nil
}

func (l *PGListener) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopCh)
	})
}

func (l *PGListener) Close(ctx context.Context) error {
	_, err := l.conn.Exec(ctx, "UNLISTEN "+l.channel)
	if err != nil {
		l.logger.Warn("failed to unlisten", "error", err)
	}
	return l.conn.Close(ctx)
}

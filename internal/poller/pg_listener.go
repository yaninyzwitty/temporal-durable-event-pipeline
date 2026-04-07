package poller

import (
	"context"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type PGListenerConfig struct {
	ConnStr string
	Channel string
	Logger  *slog.Logger
}

type PGListener struct {
	cfg      PGListenerConfig
	conn     *pgx.Conn
	channel  string
	logger   *slog.Logger
	stopCh   chan struct{}
	stopOnce sync.Once
	mu       sync.Mutex
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

func NewPGListenerFromConfig(cfg PGListenerConfig) (*PGListener, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Channel == "" {
		cfg.Channel = "events_notification"
	}

	conn, err := pgx.Connect(context.Background(), cfg.ConnStr)
	if err != nil {
		return nil, err
	}

	return &PGListener{
		cfg:     cfg,
		conn:    conn,
		channel: cfg.Channel,
		logger:  cfg.Logger,
		stopCh:  make(chan struct{}),
	}, nil
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
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn == nil {
		return nil
	}

	_, err := l.conn.Exec(ctx, "UNLISTEN "+l.channel)
	if err != nil {
		l.logger.Warn("failed to unlisten", "error", err)
	}
	closeErr := l.conn.Close(ctx)
	l.conn = nil
	return closeErr
}

func (l *PGListener) Reconnect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logger.Info("reconnecting to PostgreSQL", "channel", l.channel)

	if l.conn != nil {
		l.conn.Close(context.Background())
	}

	var err error
	l.conn, err = pgx.Connect(ctx, l.cfg.ConnStr)
	if err != nil {
		return err
	}

	_, err = l.conn.Exec(ctx, "LISTEN "+l.channel)
	if err != nil {
		return err
	}

	l.logger.Info("reconnected and subscribed to channel", "channel", l.channel)
	return nil
}

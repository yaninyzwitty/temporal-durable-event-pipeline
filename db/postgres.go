package db

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/shared/pkg"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// PoolOptions is a function type that takes a pointer to pgxpool.Config and modifies it.
type PoolOption func(*pgxpool.Config)

// WithMaxConns is a PoolOption that sets the maximum number of connections in the pool.
func WithMaxConns(maxConns int32) PoolOption {
	return func(c *pgxpool.Config) {
		c.MaxConns = maxConns
	}
}

// WithMinConns is a PoolOption that sets the minimum number of connections in the pool.
func WithMinConns(minConns int32) PoolOption {
	return func(c *pgxpool.Config) {
		c.MinConns = minConns
	}
}

// WithMaxConnLifetime is a PoolOption that sets the maximum lifetime of a connection in the pool.
func WithMaxConnLifetime(d time.Duration) PoolOption {
	return func(c *pgxpool.Config) {
		c.MaxConnLifetime = d
	}
}

// WithMaxConnIdleTime is a PoolOption that sets the maximum idle time of a connection in the pool.
func WithMaxConnIdleTime(d time.Duration) PoolOption {
	return func(c *pgxpool.Config) {
		c.MaxConnIdleTime = d
	}
}

// WithHealthCheckPeriod is a PoolOption that sets the health check period for connections in the pool.
func WithHealthCheckPeriod(d time.Duration) PoolOption {
	return func(c *pgxpool.Config) {
		c.HealthCheckPeriod = d
	}
}

// NewPool creates a new pgxpool.Pool using the provided configuration and options.
func NewPool(ctx context.Context, cfg pkg.DatabaseConfig, options ...PoolOption) (*pgxpool.Pool, error) {
	// Construct DSN using net/url for proper encoding
	pgURL := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   cfg.Name,
	}
	// Add sslmode as query parameter
	query := url.Values{}
	query.Add("sslmode", cfg.SSLMode)
	pgURL.RawQuery = query.Encode()

	poolConfig, err := pgxpool.ParseConfig(pgURL.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	// Apply additional pool options
	for _, option := range options {
		option(poolConfig)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Verify the connection to the database
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// WaitForDB attempts to create a database connection pool with retries and a delay between attempts.
func WaitForDB(ctx context.Context, cfg pkg.DatabaseConfig, maxRetries int, options ...PoolOption) (*pgxpool.Pool, error) {
	var (
		pool *pgxpool.Pool
		err  error
	)

	for i := range maxRetries {
		pool, err = NewPool(ctx, cfg, options...)
		if err == nil {
			return pool, nil
		}

		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled while waiting for database: %w", ctx.Err())
			case <-time.After(2 * time.Second):
				continue
			}

		}
	}
	return nil, fmt.Errorf("failed to connect to database after %d attempts: %w", maxRetries, err)

}

// DefaultPoolOptions returns a slice of default PoolOption settings for the connection pool.
func DefaultPoolOptions() []PoolOption {
	return []PoolOption{
		WithMaxConns(25),
		WithMinConns(5),
		WithMaxConnLifetime(30 * time.Minute),
		WithMaxConnIdleTime(5 * time.Minute),
		WithHealthCheckPeriod(time.Minute),
	}
}

// ProductionPoolOptions returns a slice of PoolOption settings optimized for production environments.
func ProductionPoolOptions() []PoolOption {
	return []PoolOption{
		WithMaxConns(100),
		WithMinConns(10),
		WithMaxConnLifetime(30 * time.Minute),
		WithMaxConnIdleTime(5 * time.Minute),
		WithHealthCheckPeriod(30 * time.Second),
	}
}

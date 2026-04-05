//go:build integration

package poller_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/poller"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/publisher"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

func skipIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION") != "true" {
		t.Skip("skipping integration test (set INTEGRATION=true to run)")
	}
}

func createPoolWithRetry(ctx context.Context, connStr string, maxRetries int) (*pgxpool.Pool, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		pool, err := repository.CreatePool(ctx, connStr)
		if err == nil {
			return pool, nil
		}
		lastErr = err
		if i < maxRetries-1 {
			backoff := time.Duration(1<<uint(i)) * time.Second
			time.Sleep(backoff)
		}
	}
	return nil, lastErr
}

func TestIntegration_OutboxPoller_WithRedpanda(t *testing.T) {
	skipIntegration(t)

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"))
	require.NoError(t, err)
	defer pgContainer.Terminate(ctx)

	pgConnStr, err := pgContainer.ConnectionString(ctx)
	require.NoError(t, err)

	pool, err := createPoolWithRetry(ctx, pgConnStr, 5)
	require.NoError(t, err)
	defer pool.Close()

	err = repository.RunMigrations(ctx, pool, "../../db/migrations")
	require.NoError(t, err)

	store := repository.NewStore(pool)

	redpandaContainer, err := redpanda.Run(ctx, "redpanda/redpanda:v26.1.1",
		redpanda.WithAutoCreateTopics())
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	broker, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	t.Logf("Redpanda broker: %s", broker)

	redpandaPublisher, err := publisher.NewRedpandaPublisher([]string{broker})
	require.NoError(t, err)
	defer redpandaPublisher.Close()

	p := poller.NewOutboxPoller(
		store.Queries,
		redpandaPublisher,
		"test-events",
		newSlogLoggerDiscard(),
		10,
		time.Millisecond*100,
	)

	pollerCtx, cancelPoller := context.WithCancel(ctx)
	go p.Start(pollerCtx)
	defer func() {
		cancelPoller()
		p.Stop()
	}()

	t.Run("should poll and publish events to Redpanda", func(t *testing.T) {
		event, err := store.CreateEvent(ctx, "test.event", []byte(`{"test":"data"}`))
		require.NoError(t, err)

		t.Logf("Created event: %s", event.ID.String())

		require.Eventually(t, func() bool {
			updatedEvent, err := store.GetEventByID(ctx, event.ID.Bytes)
			if err != nil {
				return false
			}
			return updatedEvent.Status.EventStatus == repository.EventStatusCompleted
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("should handle multiple events", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			_, err := store.CreateEvent(ctx, "batch.event", []byte(`{"index":`+string(rune('0'+i))+`}`))
			require.NoError(t, err)
		}

		require.Eventually(t, func() bool {
			events, err := store.PollPendingEvents(ctx, 10)
			if err != nil {
				return false
			}
			return len(events) == 0
		}, 5*time.Second, 100*time.Millisecond)
	})
}

func TestIntegration_Publisher_ConnectToRedpanda(t *testing.T) {
	skipIntegration(t)

	ctx := context.Background()

	redpandaContainer, err := redpanda.Run(ctx, "redpandadata/redpanda:v26.1.1",
		redpanda.WithAutoCreateTopics())
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	broker, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	t.Logf("Redpanda broker: %s", broker)

	redpandaPublisher, err := publisher.NewRedpandaPublisher([]string{broker})
	require.NoError(t, err)
	defer redpandaPublisher.Close()

	t.Run("should publish to topic", func(t *testing.T) {
		err := redpandaPublisher.Publish(ctx, "test-topic", []byte("key"), []byte(`{"message":"hello"}`))
		require.NoError(t, err)
	})
}

func TestRedpandaContainer(t *testing.T) {
	skipIntegration(t)

	ctx := context.Background()

	container, err := redpanda.Run(ctx, "redpandadata/redpanda:v26.1.1")
	require.NoError(t, err)
	defer container.Terminate(ctx)

	broker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, broker)

	t.Logf("Redpanda broker: %s", broker)
}

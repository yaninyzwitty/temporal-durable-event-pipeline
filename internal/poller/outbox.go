package poller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Close() error
}

type EventStore interface {
	PollPendingEvents(ctx context.Context, limit int32) ([]repository.Event, error)
	GetEventByID(ctx context.Context, id uuid.UUID) (repository.Event, error)
	UpdateEventStatusWithParams(ctx context.Context, arg repository.UpdateEventStatusParams) (repository.Event, error)
	MarkEventFailed(ctx context.Context, id repository.Event) (repository.Event, error)
}

type NotificationListener interface {
	Start(ctx context.Context) error
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
	Close(ctx context.Context) error
}

type OutboxListener struct {
	store       EventStore
	listener    NotificationListener
	publisher   Publisher
	topicPrefix string
	logger      *slog.Logger
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	cancel      context.CancelFunc
}

func NewOutboxListener(
	store EventStore,
	listener NotificationListener,
	publisher Publisher,
	topicPrefix string,
	logger *slog.Logger,
) *OutboxListener {
	if logger == nil {
		logger = slog.Default()
	}

	return &OutboxListener{
		store:       store,
		listener:    listener,
		publisher:   publisher,
		topicPrefix: topicPrefix,
		logger:      logger,
		stopCh:      make(chan struct{}),
	}
}

func (l *OutboxListener) Start(ctx context.Context) error {
	if err := l.listener.Start(ctx); err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}

	l.logger.Info("starting outbox listener")

	runCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	l.wg.Add(1)
	go l.run(runCtx)

	return nil
}

func (l *OutboxListener) Stop() {
	l.stopOnce.Do(func() {
		if l.cancel != nil {
			l.cancel()
		}
		close(l.stopCh)
	})
	l.wg.Wait()
	l.logger.Info("outbox listener stopped")
}

func (l *OutboxListener) run(ctx context.Context) {
	defer l.wg.Done()

	for {
		select {
		case <-ctx.Done():
			l.logger.Info("context canceled, stopping listener")
			l.listener.Close(context.Background())
			return
		case <-l.stopCh:
			l.logger.Info("stop signal received")
			l.listener.Close(context.Background())
			return
		default:
		}

		notif, err := l.listener.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil || l.isStopped() {
				return
			}
			l.logger.Error("failed to wait for notification", "error", err)
			continue
		}

		if notif == nil {
			continue
		}

		l.handleNotification(ctx, notif)
	}
}

func (l *OutboxListener) isStopped() bool {
	select {
	case <-l.stopCh:
		return true
	default:
		return false
	}
}

func (l *OutboxListener) handleNotification(ctx context.Context, notif *pgconn.Notification) {
	var payload struct {
		ID        string `json:"id"`
		EventType string `json:"event_type"`
		Status    string `json:"status"`
	}

	if err := json.Unmarshal([]byte(notif.Payload), &payload); err != nil {
		l.logger.Error("failed to parse notification payload", "error", err, "payload", notif.Payload)
		return
	}

	l.logger.Debug("received notification for event", "id", payload.ID, "type", payload.EventType)

	eventID, err := uuid.Parse(payload.ID)
	if err != nil {
		l.logger.Error("failed to parse event ID", "error", err, "id", payload.ID)
		return
	}

	event, err := l.store.GetEventByID(ctx, eventID)
	if err != nil {
		l.logger.Error("failed to get event by ID", "error", err, "id", payload.ID)
		return
	}

	l.ProcessEvent(ctx, event)
}

func (l *OutboxListener) ProcessEvent(ctx context.Context, event repository.Event) {
	topic := l.TopicForEventType(event.EventType)

	if err := l.publisher.Publish(ctx, topic, []byte(event.ID.String()), event.Payload); err != nil {
		l.logger.Error("failed to publish event", "error", err, "eventID", event.ID, "topic", topic)
		if _, updateErr := l.store.MarkEventFailed(ctx, event); updateErr != nil {
			l.logger.Error("failed to mark event as failed", "error", updateErr, "eventID", event.ID)
		}
		return
	}

	updatedEvent, err := l.store.UpdateEventStatusWithParams(ctx, repository.UpdateEventStatusParams{
		ID: event.ID,
		Status: repository.NullEventStatus{
			EventStatus: repository.EventStatusCompleted,
			Valid:       true,
		},
	})
	if err != nil {
		l.logger.Error("failed to mark event as completed", "error", err, "eventID", event.ID)
		return
	}

	l.logger.Debug("event published successfully", "eventID", updatedEvent.ID, "eventType", updatedEvent.EventType)
}

func (l *OutboxListener) TopicForEventType(eventType string) string {
	return fmt.Sprintf("%s.%s", l.topicPrefix, eventType)
}

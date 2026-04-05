package poller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
	Close() error
}

type EventQuerier interface {
	PollPendingEvents(ctx context.Context, limit int32) ([]repository.Event, error)
	UpdateEventStatus(ctx context.Context, arg repository.UpdateEventStatusParams) (repository.Event, error)
}

type OutboxPoller struct {
	store       EventQuerier
	publisher   Publisher
	topicPrefix string
	logger      *slog.Logger
	pollLimit   int32
	interval    time.Duration
	stopCh      chan struct{}
}

func NewOutboxPoller(
	store EventQuerier,
	publisher Publisher,
	topicPrefix string,
	logger *slog.Logger,
	pollLimit int32,
	interval time.Duration,
) *OutboxPoller {
	return &OutboxPoller{
		store:       store,
		publisher:   publisher,
		topicPrefix: topicPrefix,
		logger:      logger,
		pollLimit:   pollLimit,
		interval:    interval,
		stopCh:      make(chan struct{}),
	}
}

func (p *OutboxPoller) Start(ctx context.Context) {
	p.logger.Info("starting outbox poller", "pollLimit", p.pollLimit, "interval", p.interval)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("context cancelled, stopping poller")
			return
		case <-p.stopCh:
			p.logger.Info("stop signal received, stopping poller")
			return
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *OutboxPoller) Stop() {
	close(p.stopCh)
}

func (p *OutboxPoller) poll(ctx context.Context) {
	events, err := p.store.PollPendingEvents(ctx, p.pollLimit)
	if err != nil {
		p.logger.Error("failed to poll pending events", "error", err)
		return
	}

	if len(events) == 0 {
		return
	}

	p.logger.Debug("polled events", "count", len(events))

	for _, event := range events {
		p.ProcessEvent(ctx, event)
	}
}

func (p *OutboxPoller) ProcessEvent(ctx context.Context, event repository.Event) {
	topic := p.TopicForEventType(event.EventType)

	if err := p.publisher.Publish(ctx, topic, []byte(event.ID.String()), event.Payload); err != nil {
		p.logger.Error("failed to publish event", "error", err, "eventID", event.ID, "topic", topic)
		return
	}

	updatedEvent, err := p.store.UpdateEventStatus(ctx, repository.UpdateEventStatusParams{
		ID: event.ID,
		Status: repository.NullEventStatus{
			EventStatus: repository.EventStatusCompleted,
			Valid:       true,
		},
	})
	if err != nil {
		p.logger.Error("failed to mark event as completed", "error", err, "eventID", event.ID)
		return
	}

	p.logger.Debug("event published successfully", "eventID", updatedEvent.ID, "eventType", updatedEvent.EventType)
}

func (p *OutboxPoller) TopicForEventType(eventType string) string {
	return fmt.Sprintf("%s.%s", p.topicPrefix, eventType)
}

package poller_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/poller"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
)

type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Publish(ctx context.Context, topic string, key, value []byte) error {
	args := m.Called(ctx, topic, key, value)
	return args.Error(0)
}

func (m *MockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockEventQuerier struct {
	mock.Mock
}

func (m *MockEventQuerier) PollPendingEvents(ctx context.Context, limit int32) ([]repository.Event, error) {
	args := m.Called(ctx, limit)
	return args.Get(0).([]repository.Event), args.Error(1)
}

func (m *MockEventQuerier) UpdateEventStatus(ctx context.Context, arg repository.UpdateEventStatusParams) (repository.Event, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(repository.Event), args.Error(1)
}

func TestNewOutboxPoller(t *testing.T) {
	t.Run("should create poller with correct config", func(t *testing.T) {
		mockStore := new(MockEventQuerier)
		mockPublisher := new(MockPublisher)

		p := poller.NewOutboxPoller(
			mockStore,
			mockPublisher,
			"test-prefix",
			newSlogLoggerDiscard(),
			10,
			100,
		)

		assert.NotNil(t, p)
	})

	t.Run("should accept custom logger", func(t *testing.T) {
		mockStore := new(MockEventQuerier)
		mockPublisher := new(MockPublisher)
		testLogger := newSlogLoggerDiscard()

		p := poller.NewOutboxPoller(
			mockStore,
			mockPublisher,
			"prefix",
			testLogger,
			5,
			50,
		)

		assert.NotNil(t, p)
	})
}

func TestOutboxPoller_TopicForEventType(t *testing.T) {
	t.Run("should format topic with prefix", func(t *testing.T) {
		mockStore := new(MockEventQuerier)
		mockPublisher := new(MockPublisher)

		p := poller.NewOutboxPoller(
			mockStore,
			mockPublisher,
			"temporal-pipeline",
			newSlogLoggerDiscard(),
			10,
			100,
		)

		topic := p.TopicForEventType("order.created")
		assert.Equal(t, "temporal-pipeline.order.created", topic)
	})

	t.Run("should handle different event types", func(t *testing.T) {
		mockStore := new(MockEventQuerier)
		mockPublisher := new(MockPublisher)

		p := poller.NewOutboxPoller(
			mockStore,
			mockPublisher,
			"events",
			newSlogLoggerDiscard(),
			10,
			100,
		)

		assert.Equal(t, "events.payment.succeeded", p.TopicForEventType("payment.succeeded"))
		assert.Equal(t, "events.order.updated", p.TopicForEventType("order.updated"))
	})
}

func TestOutboxPoller_ProcessEvent(t *testing.T) {
	t.Run("should publish and update status on success", func(t *testing.T) {
		mockStore := new(MockEventQuerier)
		mockPublisher := new(MockPublisher)

		eventID := pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4}, Valid: true}
		testEvent := repository.Event{
			ID:        eventID,
			EventType: "test.event",
			Payload:   []byte(`{"key":"value"}`),
		}

		mockPublisher.On("Publish", context.Background(), "prefix.test.event", []byte(eventID.String()), testEvent.Payload).Return(nil)
		mockStore.On("UpdateEventStatus", context.Background(), mock.MatchedBy(func(arg repository.UpdateEventStatusParams) bool {
			return arg.ID.Bytes == eventID.Bytes &&
				arg.Status.EventStatus == repository.EventStatusCompleted &&
				arg.Status.Valid == true
		})).Return(testEvent, nil)

		p := poller.NewOutboxPoller(
			mockStore,
			mockPublisher,
			"prefix",
			newSlogLoggerDiscard(),
			10,
			100,
		)

		p.ProcessEvent(context.Background(), testEvent)

		mockPublisher.AssertExpectations(t)
		mockStore.AssertExpectations(t)
	})

	t.Run("should not update status when publish fails", func(t *testing.T) {
		mockStore := new(MockEventQuerier)
		mockPublisher := new(MockPublisher)

		eventID := pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4}, Valid: true}
		testEvent := repository.Event{
			ID:        eventID,
			EventType: "test.event",
			Payload:   []byte(`{}`),
		}

		mockPublisher.On("Publish", context.Background(), "prefix.test.event", []byte(eventID.String()), testEvent.Payload).Return(assert.AnError)

		p := poller.NewOutboxPoller(
			mockStore,
			mockPublisher,
			"prefix",
			newSlogLoggerDiscard(),
			10,
			100,
		)

		p.ProcessEvent(context.Background(), testEvent)

		mockPublisher.AssertExpectations(t)
		mockStore.AssertNotCalled(t, "UpdateEventStatus")
	})
}

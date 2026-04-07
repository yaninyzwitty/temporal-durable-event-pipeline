package poller_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
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

type MockEventStore struct {
	mock.Mock
}

func (m *MockEventStore) PollPendingEvents(ctx context.Context, limit int32) ([]repository.Event, error) {
	args := m.Called(ctx, limit)
	return args.Get(0).([]repository.Event), args.Error(1)
}

func (m *MockEventStore) GetEventByID(ctx context.Context, id uuid.UUID) (repository.Event, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(repository.Event), args.Error(1)
}

func (m *MockEventStore) UpdateEventStatusWithParams(ctx context.Context, arg repository.UpdateEventStatusParams) (repository.Event, error) {
	args := m.Called(ctx, arg)
	return args.Get(0).(repository.Event), args.Error(1)
}

func (m *MockEventStore) MarkEventFailed(ctx context.Context, event repository.Event) (repository.Event, error) {
	args := m.Called(ctx, event)
	return args.Get(0).(repository.Event), args.Error(1)
}

type MockListener struct {
	mock.Mock
}

func (m *MockListener) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockListener) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pgconn.Notification), args.Error(1)
}

func (m *MockListener) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockListener) Reconnect(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestNewOutboxListener(t *testing.T) {
	t.Run("should create listener with correct config", func(t *testing.T) {
		mockStore := new(MockEventStore)
		mockListener := new(MockListener)
		mockPublisher := new(MockPublisher)

		l := poller.NewOutboxListener(
			mockStore,
			mockListener,
			mockPublisher,
			"test-prefix",
			newSlogLoggerDiscard(),
			100,
		)

		assert.NotNil(t, l)
	})

	t.Run("should accept custom logger", func(t *testing.T) {
		mockStore := new(MockEventStore)
		mockListener := new(MockListener)
		mockPublisher := new(MockPublisher)
		testLogger := newSlogLoggerDiscard()

		l := poller.NewOutboxListener(
			mockStore,
			mockListener,
			mockPublisher,
			"prefix",
			testLogger,
			50,
		)

		assert.NotNil(t, l)
	})
}

func TestOutboxListener_TopicForEventType(t *testing.T) {
	t.Run("should format topic with prefix", func(t *testing.T) {
		mockStore := new(MockEventStore)
		mockListener := new(MockListener)
		mockPublisher := new(MockPublisher)

		l := poller.NewOutboxListener(
			mockStore,
			mockListener,
			mockPublisher,
			"temporal-pipeline",
			newSlogLoggerDiscard(),
			100,
		)

		topic := l.TopicForEventType("order.created")
		assert.Equal(t, "temporal-pipeline.order.created", topic)
	})

	t.Run("should handle different event types", func(t *testing.T) {
		mockStore := new(MockEventStore)
		mockListener := new(MockListener)
		mockPublisher := new(MockPublisher)

		l := poller.NewOutboxListener(
			mockStore,
			mockListener,
			mockPublisher,
			"events",
			newSlogLoggerDiscard(),
			100,
		)

		assert.Equal(t, "events.payment.succeeded", l.TopicForEventType("payment.succeeded"))
		assert.Equal(t, "events.order.updated", l.TopicForEventType("order.updated"))
	})
}

func TestOutboxListener_ProcessEvent(t *testing.T) {
	t.Run("should publish and update status on success", func(t *testing.T) {
		mockStore := new(MockEventStore)
		mockListener := new(MockListener)
		mockPublisher := new(MockPublisher)

		eventID := pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4}, Valid: true}
		testEvent := repository.Event{
			ID:        eventID,
			EventType: "test.event",
			Payload:   []byte(`{"key":"value"}`),
		}

		mockPublisher.On("Publish", context.Background(), "prefix.test.event", []byte(eventID.String()), testEvent.Payload).Return(nil)
		mockStore.On("UpdateEventStatusWithParams", context.Background(), mock.MatchedBy(func(arg repository.UpdateEventStatusParams) bool {
			return arg.ID.Bytes == eventID.Bytes &&
				arg.Status.EventStatus == repository.EventStatusCompleted &&
				arg.Status.Valid == true
		})).Return(testEvent, nil)

		l := poller.NewOutboxListener(
			mockStore,
			mockListener,
			mockPublisher,
			"prefix",
			newSlogLoggerDiscard(),
			100,
		)

		l.ProcessEvent(context.Background(), testEvent)

		mockPublisher.AssertExpectations(t)
		mockStore.AssertExpectations(t)
	})

	t.Run("should mark failed when publish fails", func(t *testing.T) {
		mockStore := new(MockEventStore)
		mockListener := new(MockListener)
		mockPublisher := new(MockPublisher)

		eventID := pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4}, Valid: true}
		testEvent := repository.Event{
			ID:        eventID,
			EventType: "test.event",
			Payload:   []byte(`{}`),
		}

		mockPublisher.On("Publish", context.Background(), "prefix.test.event", []byte(eventID.String()), testEvent.Payload).Return(assert.AnError)
		mockStore.On("MarkEventFailed", context.Background(), testEvent).Return(testEvent, nil)

		l := poller.NewOutboxListener(
			mockStore,
			mockListener,
			mockPublisher,
			"prefix",
			newSlogLoggerDiscard(),
			100,
		)

		l.ProcessEvent(context.Background(), testEvent)

		mockPublisher.AssertExpectations(t)
		mockStore.AssertExpectations(t)
	})
}

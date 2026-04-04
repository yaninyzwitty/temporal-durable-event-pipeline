package handler

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	eventv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/event/v1"
)

func setupEventHandler(t *testing.T) (*MockEventStore, *EventHandler) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockEventStore(ctrl)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := NewEventHandler(mockStore, logger)
	return mockStore, handler
}

func TestEventHandler_CreateEvent_Success(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	payload, _ := structpb.NewStruct(map[string]interface{}{"action": "test"})
	req := &eventv1.CreateEventRequest{
		EventType: "user.signup",
		Payload:   payload,
	}

	expectedEvent := repository.Event{
		ID:        pgtype.UUID{Bytes: uuid.New(), Valid: true},
		EventType: "user.signup",
		Payload:   []byte(`{"action":"test"}`),
		Status:    repository.NullEventStatus{EventStatus: repository.EventStatusPending, Valid: true},
	}

	mockStore.EXPECT().CreateEvent(gomock.Any(), "user.signup", gomock.Any()).Return(expectedEvent, nil)

	resp, err := handler.CreateEvent(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Event == nil {
		t.Fatal("expected event to not be nil")
	}
	if resp.Event.EventType != "user.signup" {
		t.Errorf("expected event type 'user.signup', got %s", resp.Event.EventType)
	}
}

func TestEventHandler_CreateEvent_MissingEventType(t *testing.T) {
	_, handler := setupEventHandler(t)
	ctx := context.Background()

	req := &eventv1.CreateEventRequest{
		EventType: "",
	}

	_, err := handler.CreateEvent(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestEventHandler_GetEvent_Success(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	eventID := uuid.New()
	req := &eventv1.GetEventRequest{Id: eventID.String()}

	expectedEvent := repository.Event{
		ID:        pgtype.UUID{Bytes: eventID, Valid: true},
		EventType: "user.signup",
		Status:    repository.NullEventStatus{EventStatus: repository.EventStatusPending, Valid: true},
	}

	mockStore.EXPECT().GetEventByID(gomock.Any(), eventID).Return(expectedEvent, nil)

	resp, err := handler.GetEvent(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Event == nil {
		t.Fatal("expected event to not be nil")
	}
}

func TestEventHandler_GetEvent_InvalidID(t *testing.T) {
	_, handler := setupEventHandler(t)
	ctx := context.Background()

	req := &eventv1.GetEventRequest{Id: "invalid-uuid"}

	_, err := handler.GetEvent(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestEventHandler_GetEvent_NotFound(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	eventID := uuid.New()
	req := &eventv1.GetEventRequest{Id: eventID.String()}

	mockStore.EXPECT().GetEventByID(gomock.Any(), eventID).Return(repository.Event{}, pgx.ErrNoRows)

	_, err := handler.GetEvent(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", s.Code())
	}
}

func TestEventHandler_PollPendingEvents_Success(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	events := []repository.Event{
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, EventType: "user.signup"},
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, EventType: "user.login"},
	}

	mockStore.EXPECT().PollPendingEvents(gomock.Any(), int32(10)).Return(events, nil)

	resp, err := handler.PollPendingEvents(ctx, &eventv1.PollPendingEventsRequest{Limit: 10})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(resp.Events))
	}
}

func TestEventHandler_PollPendingEvents_DefaultLimit(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	mockStore.EXPECT().PollPendingEvents(gomock.Any(), int32(10)).Return([]repository.Event{}, nil)

	_, err := handler.PollPendingEvents(ctx, &eventv1.PollPendingEventsRequest{Limit: 0})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestEventHandler_UpdateEventStatus_Success(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	eventID := uuid.New()
	req := &eventv1.UpdateEventStatusRequest{
		Id:       eventID.String(),
		StatusV2: eventv1.EventStatus_EVENT_STATUS_COMPLETED,
	}

	expectedEvent := repository.Event{
		ID:     pgtype.UUID{Bytes: eventID, Valid: true},
		Status: repository.NullEventStatus{EventStatus: repository.EventStatusCompleted, Valid: true},
	}

	mockStore.EXPECT().UpdateEventStatus(gomock.Any(), eventID, repository.EventStatusCompleted).Return(expectedEvent, nil)

	resp, err := handler.UpdateEventStatus(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Event.StatusV2 != eventv1.EventStatus_EVENT_STATUS_COMPLETED {
		t.Errorf("expected status COMPLETED, got %v", resp.Event.StatusV2)
	}
}

func TestEventHandler_UpdateEventStatus_MissingStatus(t *testing.T) {
	_, handler := setupEventHandler(t)
	ctx := context.Background()

	eventID := uuid.New()
	req := &eventv1.UpdateEventStatusRequest{
		Id:       eventID.String(),
		StatusV2: eventv1.EventStatus_EVENT_STATUS_UNSPECIFIED,
	}

	_, err := handler.UpdateEventStatus(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestEventHandler_UpdateEventStatus_InvalidID(t *testing.T) {
	_, handler := setupEventHandler(t)
	ctx := context.Background()

	req := &eventv1.UpdateEventStatusRequest{
		Id:       "invalid-uuid",
		StatusV2: eventv1.EventStatus_EVENT_STATUS_COMPLETED,
	}

	_, err := handler.UpdateEventStatus(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestEventHandler_UpdateEventStatus_NotFound(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	eventID := uuid.New()
	req := &eventv1.UpdateEventStatusRequest{
		Id:       eventID.String(),
		StatusV2: eventv1.EventStatus_EVENT_STATUS_COMPLETED,
	}

	mockStore.EXPECT().UpdateEventStatus(gomock.Any(), eventID, repository.EventStatusCompleted).Return(repository.Event{}, pgx.ErrNoRows)

	_, err := handler.UpdateEventStatus(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", s.Code())
	}
}

func TestEventHandler_ListEvents_Success(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	events := []repository.Event{
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, EventType: "user.signup"},
	}

	mockStore.EXPECT().ListEvents(gomock.Any(), int32(20), int32(0)).Return(events, nil)

	resp, err := handler.ListEvents(ctx, &eventv1.ListEventsRequest{Limit: 20, Offset: 0})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.Events) != 1 {
		t.Errorf("expected 1 event, got %d", len(resp.Events))
	}
}

func TestEventHandler_ListEvents_DefaultLimit(t *testing.T) {
	mockStore, handler := setupEventHandler(t)
	ctx := context.Background()

	mockStore.EXPECT().ListEvents(gomock.Any(), int32(20), int32(0)).Return([]repository.Event{}, nil)

	_, err := handler.ListEvents(ctx, &eventv1.ListEventsRequest{Limit: 0, Offset: 0})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

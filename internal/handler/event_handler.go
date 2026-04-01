package handler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	eventv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/event/v1"
)

type EventStore interface {
	CreateEvent(ctx context.Context, eventType string, payload []byte) (repository.Event, error)
	GetEventByID(ctx context.Context, id uuid.UUID) (repository.Event, error)
	PollPendingEvents(ctx context.Context, limit int32) ([]repository.Event, error)
	UpdateEventStatus(ctx context.Context, id uuid.UUID, status repository.EventStatus) (repository.Event, error)
	ListEvents(ctx context.Context, limit, offset int32) ([]repository.Event, error)
}

type EventHandler struct {
	eventv1.UnimplementedEventServiceServer
	store  EventStore
	logger *slog.Logger
}

func NewEventHandler(store EventStore, logger *slog.Logger) *EventHandler {
	return &EventHandler{store: store, logger: logger}
}

func (h *EventHandler) CreateEvent(ctx context.Context, req *eventv1.CreateEventRequest) (*eventv1.CreateEventResponse, error) {
	if req.GetEventType() == "" {
		return nil, status.Error(codes.InvalidArgument, "event_type is required")
	}

	var payload []byte
	if req.GetPayload() != nil {
		var err error
		payload, err = protojson.Marshal(req.GetPayload())
		if err != nil {
			h.logger.Error("failed to marshal payload", "error", err)
			return nil, status.Error(codes.InvalidArgument, "invalid payload")
		}
	}

	event, err := h.store.CreateEvent(ctx, req.GetEventType(), payload)
	if err != nil {
		h.logger.Error("failed to create event", "error", err)
		return nil, status.Error(codes.Internal, "failed to create event")
	}

	protoEvent, err := eventToProto(event)
	if err != nil {
		h.logger.Error("failed to convert event", "error", err)
		return nil, status.Error(codes.Internal, "failed to convert event")
	}

	return &eventv1.CreateEventResponse{
		Event: protoEvent,
	}, nil
}

func (h *EventHandler) GetEvent(ctx context.Context, req *eventv1.GetEventRequest) (*eventv1.GetEventResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event id")
	}

	event, err := h.store.GetEventByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "event not found")
		}
		h.logger.Error("failed to get event", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "internal error")
	}

	protoEvent, err := eventToProto(event)
	if err != nil {
		h.logger.Error("failed to convert event", "error", err)
		return nil, status.Error(codes.Internal, "failed to convert event")
	}

	return &eventv1.GetEventResponse{
		Event: protoEvent,
	}, nil
}

func (h *EventHandler) PollPendingEvents(ctx context.Context, req *eventv1.PollPendingEventsRequest) (*eventv1.PollPendingEventsResponse, error) {
	limit := req.GetLimit()
	if limit <= 0 {
		limit = 10
	}

	events, err := h.store.PollPendingEvents(ctx, limit)
	if err != nil {
		h.logger.Error("failed to poll pending events", "error", err)
		return nil, status.Error(codes.Internal, "failed to poll pending events")
	}

	result := make([]*eventv1.Event, 0, len(events))
	for _, e := range events {
		protoEvent, err := eventToProto(e)
		if err != nil {
			h.logger.Error("failed to convert event", "error", err, "event_id", pgUUIDToString(e.ID))
			return nil, status.Errorf(codes.Internal, "failed to convert event %s: %v", pgUUIDToString(e.ID), err)
		}
		result = append(result, protoEvent)
	}

	return &eventv1.PollPendingEventsResponse{Events: result}, nil
}

func (h *EventHandler) UpdateEventStatus(ctx context.Context, req *eventv1.UpdateEventStatusRequest) (*eventv1.UpdateEventStatusResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid event id")
	}
	if req.GetStatus() == "" {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}

	statusVal := repository.EventStatus(req.GetStatus())
	switch statusVal {
	case repository.EventStatusPending, repository.EventStatusProcessing, repository.EventStatusCompleted, repository.EventStatusFailed:
		// valid status
	default:
		return nil, status.Error(codes.InvalidArgument, "invalid status value; must be one of PENDING, PROCESSING, COMPLETED, FAILED")
	}
	event, err := h.store.UpdateEventStatus(ctx, id, statusVal)
	if err != nil {
		h.logger.Error("failed to update event status", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to update event status")
	}

	protoEvent, err := eventToProto(event)
	if err != nil {
		h.logger.Error("failed to convert event", "error", err)
		return nil, status.Error(codes.Internal, "failed to convert event")
	}

	return &eventv1.UpdateEventStatusResponse{
		Event: protoEvent,
	}, nil
}

func (h *EventHandler) ListEvents(ctx context.Context, req *eventv1.ListEventsRequest) (*eventv1.ListEventsResponse, error) {
	limit := req.GetLimit()
	if limit <= 0 {
		limit = 20
	}

	events, err := h.store.ListEvents(ctx, limit, req.GetOffset())
	if err != nil {
		h.logger.Error("failed to list events", "error", err)
		return nil, status.Error(codes.Internal, "failed to list events")
	}

	result := make([]*eventv1.Event, 0, len(events))
	for _, e := range events {
		protoEvent, err := eventToProto(e)
		if err != nil {
			h.logger.Error("failed to convert event", "error", err, "event_id", pgUUIDToString(e.ID))
			return nil, status.Errorf(codes.Internal, "failed to convert event %s: %v", pgUUIDToString(e.ID), err)
		}
		result = append(result, protoEvent)
	}

	return &eventv1.ListEventsResponse{Events: result}, nil
}

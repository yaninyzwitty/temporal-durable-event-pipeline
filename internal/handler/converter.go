package handler

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	eventv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/event/v1"
	orderv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/order/v1"
	productv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/product/v1"
	userv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/user/v1"
)

func pgTimestampToProto(ts pgtype.Timestamp) *timestamppb.Timestamp {
	if !ts.Valid {
		return nil
	}
	return timestamppb.New(ts.Time)
}

func pgUUIDToString(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return id.String()
}

func pgTextToString(t pgtype.Text) string {
	if !t.Valid {
		return ""
	}
	return t.String
}

func pgNumericToString(n pgtype.Numeric) string {
	if !n.Valid {
		return "0"
	}
	s, err := n.Value()
	if err != nil {
		return "0"
	}
	if s == nil {
		return "0"
	}
	str, ok := s.(string)
	if !ok {
		return "0"
	}
	return str
}

func pgInt4ToInt32(i pgtype.Int4) int32 {
	if !i.Valid {
		return 0
	}
	return i.Int32
}

func userRowToProto(u repository.CreateUserRow) *userv1.User {
	return &userv1.User{
		Id:        pgUUIDToString(u.ID),
		Username:  u.Username,
		Email:     u.Email,
		CreatedAt: pgTimestampToProto(u.CreatedAt),
		UpdatedAt: pgTimestampToProto(u.UpdatedAt),
	}
}

func userModelToProto(u repository.User) *userv1.User {
	return &userv1.User{
		Id:        pgUUIDToString(u.ID),
		Username:  u.Username,
		Email:     u.Email,
		CreatedAt: pgTimestampToProto(u.CreatedAt),
		UpdatedAt: pgTimestampToProto(u.UpdatedAt),
	}
}

func getUserByIDRowToProto(u repository.GetUserByIDRow) *userv1.User {
	return &userv1.User{
		Id:        pgUUIDToString(u.ID),
		Username:  u.Username,
		Email:     u.Email,
		CreatedAt: pgTimestampToProto(u.CreatedAt),
		UpdatedAt: pgTimestampToProto(u.UpdatedAt),
	}
}

func listUsersRowToProto(u repository.ListUsersRow) *userv1.User {
	return &userv1.User{
		Id:        pgUUIDToString(u.ID),
		Username:  u.Username,
		Email:     u.Email,
		CreatedAt: pgTimestampToProto(u.CreatedAt),
		UpdatedAt: pgTimestampToProto(u.UpdatedAt),
	}
}

func updateUserRowToProto(u repository.UpdateUserRow) *userv1.User {
	return &userv1.User{
		Id:        pgUUIDToString(u.ID),
		Username:  u.Username,
		Email:     u.Email,
		CreatedAt: pgTimestampToProto(u.CreatedAt),
		UpdatedAt: pgTimestampToProto(u.UpdatedAt),
	}
}

func productToProto(p repository.Product) *productv1.Product {
	return &productv1.Product{
		Id:            pgUUIDToString(p.ID),
		Name:          p.Name,
		Description:   pgTextToString(p.Description),
		Price:         pgNumericToString(p.Price),
		StockQuantity: pgInt4ToInt32(p.StockQuantity),
		CreatedAt:     pgTimestampToProto(p.CreatedAt),
		UpdatedAt:     pgTimestampToProto(p.UpdatedAt),
	}
}

func orderToProto(o repository.Order) *orderv1.Order {
	return &orderv1.Order{
		Id:          pgUUIDToString(o.ID),
		UserId:      pgUUIDToString(o.UserID),
		OrderDate:   pgTimestampToProto(o.OrderDate),
		Status:      pgTextToString(o.Status),
		TotalAmount: pgNumericToString(o.TotalAmount),
	}
}

func orderItemToProto(oi repository.OrderItem) *orderv1.OrderItem {
	return &orderv1.OrderItem{
		Id:        pgUUIDToString(oi.ID),
		OrderId:   pgUUIDToString(oi.OrderID),
		ProductId: pgUUIDToString(oi.ProductID),
		Quantity:  oi.Quantity,
		UnitPrice: pgNumericToString(oi.UnitPrice),
	}
}

func eventStatusToProto(s repository.EventStatus) eventv1.EventStatus {
	switch s {
	case repository.EventStatusPending:
		return eventv1.EventStatus_EVENT_STATUS_PENDING
	case repository.EventStatusProcessing:
		return eventv1.EventStatus_EVENT_STATUS_PROCESSING
	case repository.EventStatusCompleted:
		return eventv1.EventStatus_EVENT_STATUS_COMPLETED
	case repository.EventStatusFailed:
		return eventv1.EventStatus_EVENT_STATUS_FAILED
	default:
		return eventv1.EventStatus_EVENT_STATUS_UNSPECIFIED
	}
}

func eventStatusFromProto(s eventv1.EventStatus) repository.EventStatus {
	switch s {
	case eventv1.EventStatus_EVENT_STATUS_PENDING:
		return repository.EventStatusPending
	case eventv1.EventStatus_EVENT_STATUS_PROCESSING:
		return repository.EventStatusProcessing
	case eventv1.EventStatus_EVENT_STATUS_COMPLETED:
		return repository.EventStatusCompleted
	case eventv1.EventStatus_EVENT_STATUS_FAILED:
		return repository.EventStatusFailed
	default:
		return ""
	}
}

func eventToProto(e repository.Event) (*eventv1.Event, error) {
	var payload *structpb.Struct
	if len(e.Payload) > 0 {
		payload = &structpb.Struct{}
		if err := protojson.Unmarshal(e.Payload, payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event payload: %w", err)
		}
	}

	status := eventv1.EventStatus_EVENT_STATUS_UNSPECIFIED
	if e.Status.Valid {
		status = eventStatusToProto(e.Status.EventStatus)
	}

	return &eventv1.Event{
		Id:          pgUUIDToString(e.ID),
		EventType:   e.EventType,
		Payload:     payload,
		StatusV2:    status,
		CreatedAt:   pgTimestampToProto(e.CreatedAt),
		UpdatedAt:   pgTimestampToProto(e.UpdatedAt),
		ProcessedAt: pgTimestampToProto(e.ProcessedAt),
	}, nil
}

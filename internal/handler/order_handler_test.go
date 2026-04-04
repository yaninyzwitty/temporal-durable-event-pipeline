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

	orderv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/order/v1"
)

func newNumeric(s string) pgtype.Numeric {
	var n pgtype.Numeric
	n.Scan(s)
	return n
}

func setupOrderHandler(t *testing.T) (*MockOrderStore, *OrderHandler) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockOrderStore(ctrl)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := NewOrderHandler(mockStore, logger)
	return mockStore, handler
}

func TestOrderHandler_CreateOrder_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	userID := uuid.New()
	req := &orderv1.CreateOrderRequest{
		UserId:      userID.String(),
		TotalAmount: "59.99",
	}

	expectedOrder := repository.Order{
		ID:          pgtype.UUID{Bytes: uuid.New(), Valid: true},
		UserID:      pgtype.UUID{Bytes: userID, Valid: true},
		Status:      pgtype.Text{String: "pending", Valid: true},
		TotalAmount: newNumeric("59.99"),
	}

	mockStore.EXPECT().CreateOrder(gomock.Any(), userID, "59.99").Return(expectedOrder, nil)

	resp, err := handler.CreateOrder(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Order == nil {
		t.Fatal("expected order to not be nil")
	}
}

func TestOrderHandler_CreateOrder_InvalidUserID(t *testing.T) {
	_, handler := setupOrderHandler(t)
	ctx := context.Background()

	req := &orderv1.CreateOrderRequest{
		UserId:      "invalid-uuid",
		TotalAmount: "59.99",
	}

	_, err := handler.CreateOrder(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestOrderHandler_CreateOrder_MissingTotalAmount(t *testing.T) {
	_, handler := setupOrderHandler(t)
	ctx := context.Background()

	userID := uuid.New()
	req := &orderv1.CreateOrderRequest{
		UserId:      userID.String(),
		TotalAmount: "",
	}

	_, err := handler.CreateOrder(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestOrderHandler_GetOrder_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	req := &orderv1.GetOrderRequest{Id: orderID.String()}

	expectedOrder := repository.Order{
		ID:     pgtype.UUID{Bytes: orderID, Valid: true},
		Status: pgtype.Text{String: "pending", Valid: true},
	}

	mockStore.EXPECT().GetOrderByID(gomock.Any(), orderID).Return(expectedOrder, nil)

	resp, err := handler.GetOrder(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Order == nil {
		t.Fatal("expected order to not be nil")
	}
}

func TestOrderHandler_GetOrder_InvalidID(t *testing.T) {
	_, handler := setupOrderHandler(t)
	ctx := context.Background()

	req := &orderv1.GetOrderRequest{Id: "invalid-uuid"}

	_, err := handler.GetOrder(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestOrderHandler_GetOrder_NotFound(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	req := &orderv1.GetOrderRequest{Id: orderID.String()}

	mockStore.EXPECT().GetOrderByID(gomock.Any(), orderID).Return(repository.Order{}, pgx.ErrNoRows)

	_, err := handler.GetOrder(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", s.Code())
	}
}

func TestOrderHandler_ListOrdersByUser_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	userID := uuid.New()
	req := &orderv1.ListOrdersByUserRequest{UserId: userID.String()}

	orders := []repository.Order{
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, UserID: pgtype.UUID{Bytes: userID, Valid: true}},
	}

	mockStore.EXPECT().ListOrdersByUser(gomock.Any(), userID).Return(orders, nil)

	resp, err := handler.ListOrdersByUser(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.Orders) != 1 {
		t.Errorf("expected 1 order, got %d", len(resp.Orders))
	}
}

func TestOrderHandler_ListOrdersByUser_InvalidUserID(t *testing.T) {
	_, handler := setupOrderHandler(t)
	ctx := context.Background()

	req := &orderv1.ListOrdersByUserRequest{UserId: "invalid-uuid"}

	_, err := handler.ListOrdersByUser(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestOrderHandler_UpdateOrderStatus_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	req := &orderv1.UpdateOrderStatusRequest{
		Id:     orderID.String(),
		Status: "completed",
	}

	expectedOrder := repository.Order{
		ID:     pgtype.UUID{Bytes: orderID, Valid: true},
		Status: pgtype.Text{String: "completed", Valid: true},
	}

	mockStore.EXPECT().UpdateOrderStatus(gomock.Any(), orderID, gomock.Any()).Return(expectedOrder, nil)

	resp, err := handler.UpdateOrderStatus(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Order.Status != "completed" {
		t.Errorf("expected status 'completed', got %s", resp.Order.Status)
	}
}

func TestOrderHandler_UpdateOrderStatus_MissingStatus(t *testing.T) {
	_, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	req := &orderv1.UpdateOrderStatusRequest{
		Id:     orderID.String(),
		Status: "",
	}

	_, err := handler.UpdateOrderStatus(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestOrderHandler_DeleteOrder_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	req := &orderv1.DeleteOrderRequest{Id: orderID.String()}

	mockStore.EXPECT().DeleteOrder(gomock.Any(), orderID).Return(nil)

	_, err := handler.DeleteOrder(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestOrderHandler_CreateOrderItem_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	productID := uuid.New()
	req := &orderv1.CreateOrderItemRequest{
		OrderId:   orderID.String(),
		ProductId: productID.String(),
		Quantity:  2,
		UnitPrice: "29.99",
	}

	expectedItem := repository.OrderItem{
		ID:        pgtype.UUID{Bytes: uuid.New(), Valid: true},
		OrderID:   pgtype.UUID{Bytes: orderID, Valid: true},
		ProductID: pgtype.UUID{Bytes: productID, Valid: true},
		Quantity:  2,
		UnitPrice: newNumeric("29.99"),
	}

	mockStore.EXPECT().CreateOrderItem(gomock.Any(), orderID, productID, int32(2), "29.99").Return(expectedItem, nil)

	resp, err := handler.CreateOrderItem(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.OrderItem == nil {
		t.Fatal("expected order item to not be nil")
	}
}

func TestOrderHandler_CreateOrderItem_InvalidOrderID(t *testing.T) {
	_, handler := setupOrderHandler(t)
	ctx := context.Background()

	productID := uuid.New()
	req := &orderv1.CreateOrderItemRequest{
		OrderId:   "invalid-uuid",
		ProductId: productID.String(),
		Quantity:  2,
		UnitPrice: "29.99",
	}

	_, err := handler.CreateOrderItem(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestOrderHandler_CreateOrderItem_InvalidQuantity(t *testing.T) {
	_, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	productID := uuid.New()
	req := &orderv1.CreateOrderItemRequest{
		OrderId:   orderID.String(),
		ProductId: productID.String(),
		Quantity:  0,
		UnitPrice: "29.99",
	}

	_, err := handler.CreateOrderItem(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestOrderHandler_GetOrderItems_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	orderID := uuid.New()
	req := &orderv1.GetOrderItemsRequest{OrderId: orderID.String()}

	items := []repository.OrderItem{
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, OrderID: pgtype.UUID{Bytes: orderID, Valid: true}, Quantity: 2},
	}

	mockStore.EXPECT().GetOrderItemsByOrderID(gomock.Any(), orderID).Return(items, nil)

	resp, err := handler.GetOrderItems(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.OrderItems) != 1 {
		t.Errorf("expected 1 order item, got %d", len(resp.OrderItems))
	}
}

func TestOrderHandler_DeleteOrderItem_Success(t *testing.T) {
	mockStore, handler := setupOrderHandler(t)
	ctx := context.Background()

	itemID := uuid.New()
	req := &orderv1.DeleteOrderItemRequest{Id: itemID.String()}

	mockStore.EXPECT().DeleteOrderItem(gomock.Any(), itemID).Return(nil)

	_, err := handler.DeleteOrderItem(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

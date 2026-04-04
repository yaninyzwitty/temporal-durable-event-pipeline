package handler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	orderv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/order/v1"
)

type OrderStore interface {
	CreateOrder(ctx context.Context, userID uuid.UUID, totalAmount string) (repository.Order, error)
	GetOrderByID(ctx context.Context, id uuid.UUID) (repository.Order, error)
	ListOrdersByUser(ctx context.Context, userID uuid.UUID) ([]repository.Order, error)
	UpdateOrderStatus(ctx context.Context, id uuid.UUID, status pgtype.Text) (repository.Order, error)
	DeleteOrder(ctx context.Context, id uuid.UUID) error
	CreateOrderItem(ctx context.Context, orderID, productID uuid.UUID, quantity int32, unitPrice string) (repository.OrderItem, error)
	GetOrderItemsByOrderID(ctx context.Context, orderID uuid.UUID) ([]repository.OrderItem, error)
	UpdateOrderItemQuantity(ctx context.Context, id uuid.UUID, quantity int32) (repository.OrderItem, error)
	DeleteOrderItem(ctx context.Context, id uuid.UUID) error
}

type OrderHandler struct {
	orderv1.UnimplementedOrderServiceServer
	store  OrderStore
	logger *slog.Logger
}

func NewOrderHandler(store OrderStore, logger *slog.Logger) *OrderHandler {
	return &OrderHandler{store: store, logger: logger}
}

func (h *OrderHandler) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}
	if req.GetTotalAmount() == "" {
		return nil, status.Error(codes.InvalidArgument, "total_amount is required")
	}

	order, err := h.store.CreateOrder(ctx, userID, req.GetTotalAmount())
	if err != nil {
		h.logger.Error("failed to create order", "error", err)
		return nil, status.Error(codes.Internal, "failed to create order")
	}

	return &orderv1.CreateOrderResponse{
		Order: orderToProto(order),
	}, nil
}

func (h *OrderHandler) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid order id")
	}

	order, err := h.store.GetOrderByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "order not found")
		}
		h.logger.Error("failed to get order", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "internal error")
	}

	return &orderv1.GetOrderResponse{
		Order: orderToProto(order),
	}, nil
}

func (h *OrderHandler) ListOrdersByUser(ctx context.Context, req *orderv1.ListOrdersByUserRequest) (*orderv1.ListOrdersByUserResponse, error) {
	userID, err := uuid.Parse(req.GetUserId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user_id")
	}

	orders, err := h.store.ListOrdersByUser(ctx, userID)
	if err != nil {
		h.logger.Error("failed to list orders", "error", err, "user_id", req.GetUserId())
		return nil, status.Error(codes.Internal, "failed to list orders")
	}

	result := make([]*orderv1.Order, len(orders))
	for i, o := range orders {
		result[i] = orderToProto(o)
	}

	return &orderv1.ListOrdersByUserResponse{Orders: result}, nil
}

func (h *OrderHandler) UpdateOrderStatus(ctx context.Context, req *orderv1.UpdateOrderStatusRequest) (*orderv1.UpdateOrderStatusResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid order id")
	}
	if req.GetStatus() == "" {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}

	statusText := pgtype.Text{String: req.GetStatus(), Valid: true}
	order, err := h.store.UpdateOrderStatus(ctx, id, statusText)
	if err != nil {
		h.logger.Error("failed to update order status", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to update order status")
	}

	return &orderv1.UpdateOrderStatusResponse{
		Order: orderToProto(order),
	}, nil
}

func (h *OrderHandler) DeleteOrder(ctx context.Context, req *orderv1.DeleteOrderRequest) (*orderv1.DeleteOrderResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid order id")
	}

	if err := h.store.DeleteOrder(ctx, id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "order not found")
		}
		h.logger.Error("failed to delete order", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to delete order")
	}

	return &orderv1.DeleteOrderResponse{}, nil
}

func (h *OrderHandler) CreateOrderItem(ctx context.Context, req *orderv1.CreateOrderItemRequest) (*orderv1.CreateOrderItemResponse, error) {
	orderID, err := uuid.Parse(req.GetOrderId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid order_id")
	}
	productID, err := uuid.Parse(req.GetProductId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid product_id")
	}
	if req.GetUnitPrice() == "" {
		return nil, status.Error(codes.InvalidArgument, "unit_price is required")
	}

	if req.GetQuantity() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "quantity must be greater than 0")
	}

	item, err := h.store.CreateOrderItem(ctx, orderID, productID, req.GetQuantity(), req.GetUnitPrice())
	if err != nil {
		h.logger.Error("failed to create order item", "error", err)
		return nil, status.Error(codes.Internal, "failed to create order item")
	}

	return &orderv1.CreateOrderItemResponse{
		OrderItem: orderItemToProto(item),
	}, nil
}

func (h *OrderHandler) GetOrderItems(ctx context.Context, req *orderv1.GetOrderItemsRequest) (*orderv1.GetOrderItemsResponse, error) {
	orderID, err := uuid.Parse(req.GetOrderId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid order_id")
	}

	items, err := h.store.GetOrderItemsByOrderID(ctx, orderID)
	if err != nil {
		h.logger.Error("failed to get order items", "error", err, "order_id", req.GetOrderId())
		return nil, status.Error(codes.Internal, "failed to get order items")
	}

	result := make([]*orderv1.OrderItem, len(items))
	for i, item := range items {
		result[i] = orderItemToProto(item)
	}

	return &orderv1.GetOrderItemsResponse{OrderItems: result}, nil
}

func (h *OrderHandler) UpdateOrderItemQuantity(ctx context.Context, req *orderv1.UpdateOrderItemQuantityRequest) (*orderv1.UpdateOrderItemQuantityResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid order item id")
	}

	item, err := h.store.UpdateOrderItemQuantity(ctx, id, req.GetQuantity())
	if err != nil {
		h.logger.Error("failed to update order item quantity", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to update order item quantity")
	}

	return &orderv1.UpdateOrderItemQuantityResponse{
		OrderItem: orderItemToProto(item),
	}, nil
}

func (h *OrderHandler) DeleteOrderItem(ctx context.Context, req *orderv1.DeleteOrderItemRequest) (*orderv1.DeleteOrderItemResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid order item id")
	}

	if err := h.store.DeleteOrderItem(ctx, id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "order item not found")
		}
		h.logger.Error("failed to delete order item", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to delete order item")
	}

	return &orderv1.DeleteOrderItemResponse{}, nil
}

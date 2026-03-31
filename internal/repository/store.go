package repository

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	// User
	CreateUser(ctx context.Context, username, email, passwordHash string) (CreateUserRow, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (GetUserByIDRow, error)
	GetUserByEmail(ctx context.Context, email string) (User, error)
	ListUsers(ctx context.Context) ([]ListUsersRow, error)
	UpdateUser(ctx context.Context, id uuid.UUID, username, email string) (UpdateUserRow, error)
	DeleteUser(ctx context.Context, id uuid.UUID) error

	// Product
	CreateProduct(ctx context.Context, name string, description pgtype.Text, price pgtype.Numeric, stockQuantity pgtype.Int4) (Product, error)
	GetProductByID(ctx context.Context, id uuid.UUID) (Product, error)
	ListProducts(ctx context.Context) ([]Product, error)
	UpdateProduct(ctx context.Context, id uuid.UUID, name string, description pgtype.Text, price pgtype.Numeric, stockQuantity pgtype.Int4) (Product, error)
	DeleteProduct(ctx context.Context, id uuid.UUID) error

	// Order
	CreateOrder(ctx context.Context, userID uuid.UUID, totalAmount pgtype.Numeric) (Order, error)
	GetOrderByID(ctx context.Context, id uuid.UUID) (Order, error)
	ListOrdersByUser(ctx context.Context, userID uuid.UUID) ([]Order, error)
	UpdateOrderStatus(ctx context.Context, id uuid.UUID, status pgtype.Text) (Order, error)
	DeleteOrder(ctx context.Context, id uuid.UUID) error

	// Order Item
	CreateOrderItem(ctx context.Context, orderID, productID uuid.UUID, quantity int32, unitPrice pgtype.Numeric) (OrderItem, error)
	GetOrderItemsByOrderID(ctx context.Context, orderID uuid.UUID) ([]OrderItem, error)
	UpdateOrderItemQuantity(ctx context.Context, id uuid.UUID, quantity int32) (OrderItem, error)
	DeleteOrderItem(ctx context.Context, id uuid.UUID) error

	// Event
	CreateEvent(ctx context.Context, eventType string, payload json.RawMessage) (Event, error)
	GetEventByID(ctx context.Context, id uuid.UUID) (Event, error)
	PollPendingEvents(ctx context.Context, limit int32) ([]Event, error)
	UpdateEventStatus(ctx context.Context, id uuid.UUID, status pgtype.Text) (Event, error)
	ListEvents(ctx context.Context, limit, offset int32) ([]Event, error)

	// Transaction
	ExecTx(ctx context.Context, fn func(Store) error) error
}

type store struct {
	pool *pgxpool.Pool
	*Queries
}

func NewStore(pool *pgxpool.Pool) Store {
	return &store{
		pool:    pool,
		Queries: New(pool),
	}
}

func (s *store) ExecTx(ctx context.Context, fn func(Store) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	txStore := &store{
		pool:    s.pool,
		Queries: s.Queries.WithTx(tx),
	}

	if err := fn(txStore); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// --- pgtype helpers ---

func toPgUUID(id uuid.UUID) pgtype.UUID {
	return pgtype.UUID{
		Bytes: id,
		Valid: true,
	}
}

// --- User ---

func (s *store) CreateUser(ctx context.Context, username, email, passwordHash string) (CreateUserRow, error) {
	return s.Queries.CreateUser(ctx, CreateUserParams{
		Username:     username,
		Email:        email,
		PasswordHash: passwordHash,
	})
}

func (s *store) GetUserByID(ctx context.Context, id uuid.UUID) (GetUserByIDRow, error) {
	return s.Queries.GetUserByID(ctx, toPgUUID(id))
}

func (s *store) GetUserByEmail(ctx context.Context, email string) (User, error) {
	return s.Queries.GetUserByEmail(ctx, email)
}

func (s *store) ListUsers(ctx context.Context) ([]ListUsersRow, error) {
	return s.Queries.ListUsers(ctx)
}

func (s *store) UpdateUser(ctx context.Context, id uuid.UUID, username, email string) (UpdateUserRow, error) {
	return s.Queries.UpdateUser(ctx, UpdateUserParams{
		ID:       toPgUUID(id),
		Username: username,
		Email:    email,
	})
}

func (s *store) DeleteUser(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteUser(ctx, toPgUUID(id))
}

// --- Product ---

func (s *store) CreateProduct(ctx context.Context, name string, description pgtype.Text, price pgtype.Numeric, stockQuantity pgtype.Int4) (Product, error) {
	return s.Queries.CreateProduct(ctx, CreateProductParams{
		Name:          name,
		Description:   description,
		Price:         price,
		StockQuantity: stockQuantity,
	})
}

func (s *store) GetProductByID(ctx context.Context, id uuid.UUID) (Product, error) {
	return s.Queries.GetProductByID(ctx, toPgUUID(id))
}

func (s *store) ListProducts(ctx context.Context) ([]Product, error) {
	return s.Queries.ListProducts(ctx)
}

func (s *store) UpdateProduct(ctx context.Context, id uuid.UUID, name string, description pgtype.Text, price pgtype.Numeric, stockQuantity pgtype.Int4) (Product, error) {
	return s.Queries.UpdateProduct(ctx, UpdateProductParams{
		ID:            toPgUUID(id),
		Name:          name,
		Description:   description,
		Price:         price,
		StockQuantity: stockQuantity,
	})
}

func (s *store) DeleteProduct(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteProduct(ctx, toPgUUID(id))
}

// --- Order ---

func (s *store) CreateOrder(ctx context.Context, userID uuid.UUID, totalAmount pgtype.Numeric) (Order, error) {
	return s.Queries.CreateOrder(ctx, CreateOrderParams{
		UserID:      toPgUUID(userID),
		TotalAmount: totalAmount,
	})
}

func (s *store) GetOrderByID(ctx context.Context, id uuid.UUID) (Order, error) {
	return s.Queries.GetOrderByID(ctx, toPgUUID(id))
}

func (s *store) ListOrdersByUser(ctx context.Context, userID uuid.UUID) ([]Order, error) {
	return s.Queries.ListOrdersByUser(ctx, toPgUUID(userID))
}

func (s *store) UpdateOrderStatus(ctx context.Context, id uuid.UUID, status pgtype.Text) (Order, error) {
	return s.Queries.UpdateOrderStatus(ctx, UpdateOrderStatusParams{
		ID:     toPgUUID(id),
		Status: status,
	})
}

func (s *store) DeleteOrder(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteOrder(ctx, toPgUUID(id))
}

// --- Order Item ---

func (s *store) CreateOrderItem(ctx context.Context, orderID, productID uuid.UUID, quantity int32, unitPrice pgtype.Numeric) (OrderItem, error) {
	return s.Queries.CreateOrderItem(ctx, CreateOrderItemParams{
		OrderID:   toPgUUID(orderID),
		ProductID: toPgUUID(productID),
		Quantity:  quantity,
		UnitPrice: unitPrice,
	})
}

func (s *store) GetOrderItemsByOrderID(ctx context.Context, orderID uuid.UUID) ([]OrderItem, error) {
	return s.Queries.GetOrderItemsByOrderID(ctx, toPgUUID(orderID))
}

func (s *store) UpdateOrderItemQuantity(ctx context.Context, id uuid.UUID, quantity int32) (OrderItem, error) {
	return s.Queries.UpdateOrderItemQuantity(ctx, UpdateOrderItemQuantityParams{
		ID:       toPgUUID(id),
		Quantity: quantity,
	})
}

func (s *store) DeleteOrderItem(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteOrderItem(ctx, toPgUUID(id))
}

// --- Event ---

func (s *store) CreateEvent(ctx context.Context, eventType string, payload json.RawMessage) (Event, error) {
	return s.Queries.CreateEvent(ctx, CreateEventParams{
		EventType: eventType,
		Payload:   payload,
	})
}

func (s *store) GetEventByID(ctx context.Context, id uuid.UUID) (Event, error) {
	return s.Queries.GetEventByID(ctx, toPgUUID(id))
}

func (s *store) PollPendingEvents(ctx context.Context, limit int32) ([]Event, error) {
	return s.Queries.PollPendingEvents(ctx, limit)
}

func (s *store) UpdateEventStatus(ctx context.Context, id uuid.UUID, status pgtype.Text) (Event, error) {
	return s.Queries.UpdateEventStatus(ctx, UpdateEventStatusParams{
		ID:     toPgUUID(id),
		Status: status,
	})
}

func (s *store) ListEvents(ctx context.Context, limit, offset int32) ([]Event, error) {
	return s.Queries.ListEvents(ctx, ListEventsParams{
		Limit:  limit,
		Offset: offset,
	})
}

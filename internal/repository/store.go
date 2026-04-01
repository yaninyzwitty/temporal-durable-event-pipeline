package repository

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
	*Queries
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{
		pool:    pool,
		Queries: New(pool),
	}
}

func (s *Store) ExecTx(ctx context.Context, fn func(*Store) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	txStore := &Store{
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

func toPgNumeric(s string) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	if err := n.Scan(s); err != nil {
		return pgtype.Numeric{}, fmt.Errorf("invalid numeric value %q: %w", s, err)
	}
	return n, nil
}

// --- User ---

func (s *Store) CreateUser(ctx context.Context, username, email, passwordHash string) (CreateUserRow, error) {
	return s.Queries.CreateUser(ctx, CreateUserParams{
		Username:     username,
		Email:        email,
		PasswordHash: passwordHash,
	})
}

func (s *Store) GetUserByID(ctx context.Context, id uuid.UUID) (GetUserByIDRow, error) {
	return s.Queries.GetUserByID(ctx, toPgUUID(id))
}

func (s *Store) GetUserByEmail(ctx context.Context, email string) (User, error) {
	return s.Queries.GetUserByEmail(ctx, email)
}

func (s *Store) ListUsers(ctx context.Context) ([]ListUsersRow, error) {
	return s.Queries.ListUsers(ctx)
}

func (s *Store) UpdateUser(ctx context.Context, id uuid.UUID, username, email string) (UpdateUserRow, error) {
	return s.Queries.UpdateUser(ctx, UpdateUserParams{
		ID:       toPgUUID(id),
		Username: username,
		Email:    email,
	})
}

func (s *Store) DeleteUser(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteUser(ctx, toPgUUID(id))
}

// --- Product ---

func (s *Store) CreateProduct(ctx context.Context, name string, description pgtype.Text, price string, stockQuantity pgtype.Int4) (Product, error) {
	numericPrice, err := toPgNumeric(price)
	if err != nil {
		return Product{}, err
	}
	return s.Queries.CreateProduct(ctx, CreateProductParams{
		Name:          name,
		Description:   description,
		Price:         numericPrice,
		StockQuantity: stockQuantity,
	})
}

func (s *Store) GetProductByID(ctx context.Context, id uuid.UUID) (Product, error) {
	return s.Queries.GetProductByID(ctx, toPgUUID(id))
}

func (s *Store) ListProducts(ctx context.Context) ([]Product, error) {
	return s.Queries.ListProducts(ctx)
}

func (s *Store) UpdateProduct(ctx context.Context, id uuid.UUID, name string, description pgtype.Text, price string, stockQuantity pgtype.Int4) (Product, error) {
	numericPrice, err := toPgNumeric(price)
	if err != nil {
		return Product{}, err
	}
	return s.Queries.UpdateProduct(ctx, UpdateProductParams{
		ID:            toPgUUID(id),
		Name:          name,
		Description:   description,
		Price:         numericPrice,
		StockQuantity: stockQuantity,
	})
}

func (s *Store) DeleteProduct(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteProduct(ctx, toPgUUID(id))
}

// --- Order ---

func (s *Store) CreateOrder(ctx context.Context, userID uuid.UUID, totalAmount string) (Order, error) {
	numericTotal, err := toPgNumeric(totalAmount)
	if err != nil {
		return Order{}, err
	}
	return s.Queries.CreateOrder(ctx, CreateOrderParams{
		UserID:      toPgUUID(userID),
		TotalAmount: numericTotal,
	})
}

func (s *Store) GetOrderByID(ctx context.Context, id uuid.UUID) (Order, error) {
	return s.Queries.GetOrderByID(ctx, toPgUUID(id))
}

func (s *Store) ListOrdersByUser(ctx context.Context, userID uuid.UUID) ([]Order, error) {
	return s.Queries.ListOrdersByUser(ctx, toPgUUID(userID))
}

func (s *Store) UpdateOrderStatus(ctx context.Context, id uuid.UUID, status pgtype.Text) (Order, error) {
	return s.Queries.UpdateOrderStatus(ctx, UpdateOrderStatusParams{
		ID:     toPgUUID(id),
		Status: status,
	})
}

func (s *Store) DeleteOrder(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteOrder(ctx, toPgUUID(id))
}

// --- Order Item ---

func (s *Store) CreateOrderItem(ctx context.Context, orderID, productID uuid.UUID, quantity int32, unitPrice string) (OrderItem, error) {
	numericPrice, err := toPgNumeric(unitPrice)
	if err != nil {
		return OrderItem{}, err
	}
	return s.Queries.CreateOrderItem(ctx, CreateOrderItemParams{
		OrderID:   toPgUUID(orderID),
		ProductID: toPgUUID(productID),
		Quantity:  quantity,
		UnitPrice: numericPrice,
	})
}

func (s *Store) GetOrderItemsByOrderID(ctx context.Context, orderID uuid.UUID) ([]OrderItem, error) {
	return s.Queries.GetOrderItemsByOrderID(ctx, toPgUUID(orderID))
}

func (s *Store) UpdateOrderItemQuantity(ctx context.Context, id uuid.UUID, quantity int32) (OrderItem, error) {
	return s.Queries.UpdateOrderItemQuantity(ctx, UpdateOrderItemQuantityParams{
		ID:       toPgUUID(id),
		Quantity: quantity,
	})
}

func (s *Store) DeleteOrderItem(ctx context.Context, id uuid.UUID) error {
	return s.Queries.DeleteOrderItem(ctx, toPgUUID(id))
}

// --- Event ---

func (s *Store) CreateEvent(ctx context.Context, eventType string, payload []byte) (Event, error) {
	return s.Queries.CreateEvent(ctx, CreateEventParams{
		EventType: eventType,
		Payload:   payload,
	})
}

func (s *Store) GetEventByID(ctx context.Context, id uuid.UUID) (Event, error) {
	return s.Queries.GetEventByID(ctx, toPgUUID(id))
}

func (s *Store) PollPendingEvents(ctx context.Context, limit int32) ([]Event, error) {
	return s.Queries.PollPendingEvents(ctx, limit)
}

func (s *Store) UpdateEventStatus(ctx context.Context, id uuid.UUID, status EventStatus) (Event, error) {
	switch status {
	case EventStatusPending, EventStatusProcessing, EventStatusCompleted, EventStatusFailed:
	default:
		return Event{}, fmt.Errorf("invalid event status: %q", status)
	}
	return s.Queries.UpdateEventStatus(ctx, UpdateEventStatusParams{
		ID:     toPgUUID(id),
		Status: NullEventStatus{EventStatus: status, Valid: true},
	})
}

func (s *Store) ListEvents(ctx context.Context, limit, offset int32) ([]Event, error) {
	return s.Queries.ListEvents(ctx, ListEventsParams{
		Limit:  limit,
		Offset: offset,
	})
}

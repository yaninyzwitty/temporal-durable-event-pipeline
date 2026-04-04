package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func createPoolWithRetry(ctx context.Context, connStr string, maxRetries int) (*pgxpool.Pool, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		pool, err := repository.CreatePool(ctx, connStr)
		if err == nil {
			return pool, nil
		}
		lastErr = err
		if i < maxRetries-1 {
			backoff := time.Duration(1<<uint(i)) * time.Second
			time.Sleep(backoff)
		}
	}
	return nil, lastErr
}

func TestPostgresContainer(t *testing.T) {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithDatabase("testdb"), postgres.WithUsername("testuser"), postgres.WithPassword("testpass"))
	require.NoError(t, err)
	defer container.Terminate(ctx)

	containerEndpoint, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	t.Logf("postgres container ready: %s", containerEndpoint)
}

func TestStore_User_CRUD(t *testing.T) {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithDatabase("testdb"), postgres.WithUsername("testuser"), postgres.WithPassword("testpass"))
	require.NoError(t, err)
	defer container.Terminate(ctx)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	pool, err := createPoolWithRetry(ctx, connStr, 5)
	require.NoError(t, err)
	defer pool.Close()

	err = repository.RunMigrations(ctx, pool, "../../db/migrations")
	require.NoError(t, err)

	store := repository.NewStore(pool)

	t.Run("CreateUser", func(t *testing.T) {
		user, err := store.CreateUser(ctx, "testuser", "test@example.com", "hash123")
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, user.ID.Bytes)
		assert.Equal(t, "testuser", user.Username)
		assert.Equal(t, "test@example.com", user.Email)
	})

	t.Run("GetUserByID", func(t *testing.T) {
		created, err := store.CreateUser(ctx, "user1", "user1@example.com", "hash")
		require.NoError(t, err)

		fetched, err := store.GetUserByID(ctx, created.ID.Bytes)
		require.NoError(t, err)
		assert.Equal(t, created.ID, fetched.ID)
		assert.Equal(t, "user1", fetched.Username)
	})

	t.Run("GetUserByEmail", func(t *testing.T) {
		_, err := store.CreateUser(ctx, "emailuser", "email@test.com", "hash")
		require.NoError(t, err)

		fetched, err := store.GetUserByEmail(ctx, "email@test.com")
		require.NoError(t, err)
		assert.Equal(t, "emailuser", fetched.Username)
	})

	t.Run("ListUsers", func(t *testing.T) {
		_, err := store.CreateUser(ctx, "listuser1", "list1@test.com", "hash")
		require.NoError(t, err)
		_, err = store.CreateUser(ctx, "listuser2", "list2@test.com", "hash")
		require.NoError(t, err)

		users, err := store.ListUsers(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(users), 2)
	})

	t.Run("UpdateUser", func(t *testing.T) {
		created, err := store.CreateUser(ctx, "oldname", "old@test.com", "hash")
		require.NoError(t, err)

		updated, err := store.UpdateUser(ctx, created.ID.Bytes, "newname", "new@test.com")
		require.NoError(t, err)
		assert.Equal(t, "newname", updated.Username)
		assert.Equal(t, "new@test.com", updated.Email)
	})

	t.Run("DeleteUser", func(t *testing.T) {
		created, err := store.CreateUser(ctx, "todelete", "delete@test.com", "hash")
		require.NoError(t, err)

		err = store.DeleteUser(ctx, created.ID.Bytes)
		require.NoError(t, err)

		_, err = store.GetUserByID(ctx, created.ID.Bytes)
		assert.Error(t, err)
	})
}

func TestStore_Product_CRUD(t *testing.T) {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithDatabase("testdb"), postgres.WithUsername("testuser"), postgres.WithPassword("testpass"))
	require.NoError(t, err)
	defer container.Terminate(ctx)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	pool, err := createPoolWithRetry(ctx, connStr, 5)
	require.NoError(t, err)
	defer pool.Close()

	err = repository.RunMigrations(ctx, pool, "../../db/migrations")
	require.NoError(t, err)

	store := repository.NewStore(pool)

	t.Run("CreateProduct", func(t *testing.T) {
		desc := pgtype.Text{String: "A great product", Valid: true}
		stock := pgtype.Int4{Int32: 10, Valid: true}

		product, err := store.CreateProduct(ctx, "Test Product", desc, "29.99", stock)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, product.ID.Bytes)
		assert.Equal(t, "Test Product", product.Name)
	})

	t.Run("GetProductByID", func(t *testing.T) {
		desc := pgtype.Text{String: "Test", Valid: true}
		stock := pgtype.Int4{Int32: 5, Valid: true}
		created, err := store.CreateProduct(ctx, "Prod1", desc, "10.00", stock)
		require.NoError(t, err)

		fetched, err := store.GetProductByID(ctx, created.ID.Bytes)
		require.NoError(t, err)
		assert.Equal(t, created.ID, fetched.ID)
	})

	t.Run("ListProducts", func(t *testing.T) {
		desc := pgtype.Text{Valid: false}
		stock := pgtype.Int4{Int32: 0, Valid: true}
		_, err := store.CreateProduct(ctx, "P1", desc, "5.00", stock)
		require.NoError(t, err)
		_, err = store.CreateProduct(ctx, "P2", desc, "15.00", stock)
		require.NoError(t, err)

		products, err := store.ListProducts(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(products), 2)
	})

	t.Run("UpdateProduct", func(t *testing.T) {
		desc := pgtype.Text{Valid: false}
		stock := pgtype.Int4{Int32: 0, Valid: true}
		created, err := store.CreateProduct(ctx, "OldName", desc, "5.00", stock)
		require.NoError(t, err)

		desc2 := pgtype.Text{String: "New desc", Valid: true}
		stock2 := pgtype.Int4{Int32: 20, Valid: true}
		updated, err := store.UpdateProduct(ctx, created.ID.Bytes, "NewName", desc2, "19.99", stock2)
		require.NoError(t, err)
		assert.Equal(t, "NewName", updated.Name)
	})

	t.Run("DeleteProduct", func(t *testing.T) {
		desc := pgtype.Text{Valid: false}
		stock := pgtype.Int4{Int32: 0, Valid: true}
		created, err := store.CreateProduct(ctx, "ToDelete", desc, "5.00", stock)
		require.NoError(t, err)

		err = store.DeleteProduct(ctx, created.ID.Bytes)
		require.NoError(t, err)

		_, err = store.GetProductByID(ctx, created.ID.Bytes)
		assert.Error(t, err)
	})
}

func TestStore_Event_CRUD(t *testing.T) {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithDatabase("testdb"), postgres.WithUsername("testuser"), postgres.WithPassword("testpass"))
	require.NoError(t, err)
	defer container.Terminate(ctx)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	pool, err := createPoolWithRetry(ctx, connStr, 5)
	require.NoError(t, err)
	defer pool.Close()

	err = repository.RunMigrations(ctx, pool, "../../db/migrations")
	require.NoError(t, err)

	store := repository.NewStore(pool)

	t.Run("CreateEvent", func(t *testing.T) {
		payload := []byte(`{"key":"value"}`)
		event, err := store.CreateEvent(ctx, "test_event", payload)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, event.ID.Bytes)
		assert.Equal(t, "test_event", event.EventType)
		assert.Equal(t, repository.EventStatusPending, event.Status.EventStatus)
	})

	t.Run("PollPendingEvents", func(t *testing.T) {
		_, _ = store.CreateEvent(ctx, "event1", []byte("{}"))
		_, _ = store.CreateEvent(ctx, "event2", []byte("{}"))

		events, err := store.PollPendingEvents(ctx, 10)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(events), 2)
	})

	t.Run("UpdateEventStatus", func(t *testing.T) {
		created, _ := store.CreateEvent(ctx, "status_test", []byte("{}"))

		updated, err := store.UpdateEventStatus(ctx, created.ID.Bytes, repository.EventStatusCompleted)
		require.NoError(t, err)
		assert.Equal(t, repository.EventStatusCompleted, updated.Status.EventStatus)
	})

	t.Run("ListEvents", func(t *testing.T) {
		_, _ = store.CreateEvent(ctx, "list_event1", []byte("{}"))
		_, _ = store.CreateEvent(ctx, "list_event2", []byte("{}"))

		events, err := store.ListEvents(ctx, 10, 0)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(events), 2)
	})
}

func TestStore_Transaction(t *testing.T) {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine", postgres.WithDatabase("testdb"), postgres.WithUsername("testuser"), postgres.WithPassword("testpass"))
	require.NoError(t, err)
	defer container.Terminate(ctx)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	pool, err := createPoolWithRetry(ctx, connStr, 5)
	require.NoError(t, err)
	defer pool.Close()

	err = repository.RunMigrations(ctx, pool, "../../db/migrations")
	require.NoError(t, err)

	store := repository.NewStore(pool)

	user, err := store.CreateUser(ctx, "orderuser", "order@test.com", "hash")
	require.NoError(t, err)

	product, err := store.CreateProduct(ctx, "OrderProd", pgtype.Text{Valid: false}, "10.00", pgtype.Int4{Int32: 5, Valid: true})
	require.NoError(t, err)

	err = store.ExecTx(ctx, func(s *repository.Store) error {
		order, err := s.CreateOrder(ctx, user.ID.Bytes, "10.00")
		if err != nil {
			return err
		}
		_, err = s.CreateOrderItem(ctx, order.ID.Bytes, product.ID.Bytes, 1, "10.00")
		return err
	})
	require.NoError(t, err)

	orders, err := store.ListOrdersByUser(ctx, user.ID.Bytes)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(orders), 1)
}

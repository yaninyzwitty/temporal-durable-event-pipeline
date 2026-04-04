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

	productv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/product/v1"
)

func setupProductHandler(t *testing.T) (*MockProductStore, *ProductHandler) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockProductStore(ctrl)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := NewProductHandler(mockStore, logger)
	return mockStore, handler
}

func TestProductHandler_CreateProduct_Success(t *testing.T) {
	mockStore, handler := setupProductHandler(t)
	ctx := context.Background()

	req := &productv1.CreateProductRequest{
		Name:          "Test Product",
		Description:   "A test product",
		Price:         "29.99",
		StockQuantity: 100,
	}

	expectedProduct := repository.Product{
		ID:            pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Name:          "Test Product",
		Description:   pgtype.Text{String: "A test product", Valid: true},
		Price:         newNumeric("29.99"),
		StockQuantity: pgtype.Int4{Int32: 100, Valid: true},
	}

	mockStore.EXPECT().CreateProduct(gomock.Any(), "Test Product", gomock.Any(), "29.99", gomock.Any()).Return(expectedProduct, nil)

	resp, err := handler.CreateProduct(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Product == nil {
		t.Fatal("expected product to not be nil")
	}
	if resp.Product.Name != "Test Product" {
		t.Errorf("expected name 'Test Product', got %s", resp.Product.Name)
	}
}

func TestProductHandler_CreateProduct_MissingFields(t *testing.T) {
	_, handler := setupProductHandler(t)
	ctx := context.Background()

	tests := []struct {
		name string
		req  *productv1.CreateProductRequest
	}{
		{"missing name", &productv1.CreateProductRequest{Price: "29.99"}},
		{"missing price", &productv1.CreateProductRequest{Name: "Test Product"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler.CreateProduct(ctx, tt.req)
			s, _ := status.FromError(err)
			if s.Code() != codes.InvalidArgument {
				t.Errorf("expected InvalidArgument, got %v", s.Code())
			}
		})
	}
}

func TestProductHandler_GetProduct_Success(t *testing.T) {
	mockStore, handler := setupProductHandler(t)
	ctx := context.Background()

	productID := uuid.New()
	req := &productv1.GetProductRequest{Id: productID.String()}

	expectedProduct := repository.Product{
		ID:            pgtype.UUID{Bytes: productID, Valid: true},
		Name:          "Test Product",
		Description:   pgtype.Text{String: "Description", Valid: true},
		Price:         newNumeric("29.99"),
		StockQuantity: pgtype.Int4{Int32: 100, Valid: true},
	}

	mockStore.EXPECT().GetProductByID(gomock.Any(), productID).Return(expectedProduct, nil)

	resp, err := handler.GetProduct(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Product == nil {
		t.Fatal("expected product to not be nil")
	}
}

func TestProductHandler_GetProduct_InvalidID(t *testing.T) {
	_, handler := setupProductHandler(t)
	ctx := context.Background()

	req := &productv1.GetProductRequest{Id: "invalid-uuid"}

	_, err := handler.GetProduct(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestProductHandler_GetProduct_NotFound(t *testing.T) {
	mockStore, handler := setupProductHandler(t)
	ctx := context.Background()

	productID := uuid.New()
	req := &productv1.GetProductRequest{Id: productID.String()}

	mockStore.EXPECT().GetProductByID(gomock.Any(), productID).Return(repository.Product{}, pgx.ErrNoRows)

	_, err := handler.GetProduct(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", s.Code())
	}
}

func TestProductHandler_ListProducts_Success(t *testing.T) {
	mockStore, handler := setupProductHandler(t)
	ctx := context.Background()

	products := []repository.Product{
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, Name: "Product 1", Price: newNumeric("10.00")},
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, Name: "Product 2", Price: newNumeric("20.00")},
	}

	mockStore.EXPECT().ListProducts(gomock.Any()).Return(products, nil)

	resp, err := handler.ListProducts(ctx, &productv1.ListProductsRequest{})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.Products) != 2 {
		t.Errorf("expected 2 products, got %d", len(resp.Products))
	}
}

func TestProductHandler_UpdateProduct_Success(t *testing.T) {
	mockStore, handler := setupProductHandler(t)
	ctx := context.Background()

	productID := uuid.New()
	req := &productv1.UpdateProductRequest{
		Id:            productID.String(),
		Name:          "Updated Product",
		Description:   "Updated description",
		Price:         "49.99",
		StockQuantity: 200,
	}

	expectedProduct := repository.Product{
		ID:            pgtype.UUID{Bytes: productID, Valid: true},
		Name:          "Updated Product",
		Description:   pgtype.Text{String: "Updated description", Valid: true},
		Price:         newNumeric("49.99"),
		StockQuantity: pgtype.Int4{Int32: 200, Valid: true},
	}

	mockStore.EXPECT().UpdateProduct(gomock.Any(), productID, "Updated Product", gomock.Any(), "49.99", gomock.Any()).Return(expectedProduct, nil)

	resp, err := handler.UpdateProduct(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.Product.Name != "Updated Product" {
		t.Errorf("expected name 'Updated Product', got %s", resp.Product.Name)
	}
}

func TestProductHandler_UpdateProduct_InvalidID(t *testing.T) {
	_, handler := setupProductHandler(t)
	ctx := context.Background()

	req := &productv1.UpdateProductRequest{
		Id:    "invalid-id",
		Name:  "Updated Product",
		Price: "49.99",
	}

	_, err := handler.UpdateProduct(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestProductHandler_DeleteProduct_Success(t *testing.T) {
	mockStore, handler := setupProductHandler(t)
	ctx := context.Background()

	productID := uuid.New()
	req := &productv1.DeleteProductRequest{Id: productID.String()}

	mockStore.EXPECT().DeleteProduct(gomock.Any(), productID).Return(nil)

	_, err := handler.DeleteProduct(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestProductHandler_DeleteProduct_InvalidID(t *testing.T) {
	_, handler := setupProductHandler(t)
	ctx := context.Background()

	req := &productv1.DeleteProductRequest{Id: "invalid-id"}

	_, err := handler.DeleteProduct(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

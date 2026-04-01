package handler

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	productv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/product/v1"
)

type ProductStore interface {
	CreateProduct(ctx context.Context, name string, description pgtype.Text, price string, stockQuantity pgtype.Int4) (repository.Product, error)
	GetProductByID(ctx context.Context, id uuid.UUID) (repository.Product, error)
	ListProducts(ctx context.Context) ([]repository.Product, error)
	UpdateProduct(ctx context.Context, id uuid.UUID, name string, description pgtype.Text, price string, stockQuantity pgtype.Int4) (repository.Product, error)
	DeleteProduct(ctx context.Context, id uuid.UUID) error
}

type ProductHandler struct {
	productv1.UnimplementedProductServiceServer
	store  ProductStore
	logger *slog.Logger
}

func NewProductHandler(store ProductStore, logger *slog.Logger) *ProductHandler {
	return &ProductHandler{store: store, logger: logger}
}

func (h *ProductHandler) CreateProduct(ctx context.Context, req *productv1.CreateProductRequest) (*productv1.CreateProductResponse, error) {
	if req.GetName() == "" || req.GetPrice() == "" {
		return nil, status.Error(codes.InvalidArgument, "name and price are required")
	}

	description := pgtype.Text{String: req.GetDescription(), Valid: req.GetDescription() != ""}
	stockQuantity := pgtype.Int4{Int32: req.GetStockQuantity(), Valid: true}

	product, err := h.store.CreateProduct(ctx, req.GetName(), description, req.GetPrice(), stockQuantity)
	if err != nil {
		h.logger.Error("failed to create product", "error", err)
		return nil, status.Error(codes.Internal, "failed to create product")
	}

	return &productv1.CreateProductResponse{
		Product: productToProto(product),
	}, nil
}

func (h *ProductHandler) GetProduct(ctx context.Context, req *productv1.GetProductRequest) (*productv1.GetProductResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid product id")
	}

	product, err := h.store.GetProductByID(ctx, id)
	if err != nil {
		h.logger.Error("failed to get product", "error", err, "id", req.GetId())
		return nil, status.Error(codes.NotFound, "product not found")
	}

	return &productv1.GetProductResponse{
		Product: productToProto(product),
	}, nil
}

func (h *ProductHandler) ListProducts(ctx context.Context, _ *productv1.ListProductsRequest) (*productv1.ListProductsResponse, error) {
	products, err := h.store.ListProducts(ctx)
	if err != nil {
		h.logger.Error("failed to list products", "error", err)
		return nil, status.Error(codes.Internal, "failed to list products")
	}

	result := make([]*productv1.Product, len(products))
	for i, p := range products {
		result[i] = productToProto(p)
	}

	return &productv1.ListProductsResponse{Products: result}, nil
}

func (h *ProductHandler) UpdateProduct(ctx context.Context, req *productv1.UpdateProductRequest) (*productv1.UpdateProductResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid product id")
	}
	if req.GetName() == "" || req.GetPrice() == "" {
		return nil, status.Error(codes.InvalidArgument, "name and price are required")
	}

	description := pgtype.Text{String: req.GetDescription(), Valid: req.GetDescription() != ""}
	stockQuantity := pgtype.Int4{Int32: req.GetStockQuantity(), Valid: true}

	product, err := h.store.UpdateProduct(ctx, id, req.GetName(), description, req.GetPrice(), stockQuantity)
	if err != nil {
		h.logger.Error("failed to update product", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to update product")
	}

	return &productv1.UpdateProductResponse{
		Product: productToProto(product),
	}, nil
}

func (h *ProductHandler) DeleteProduct(ctx context.Context, req *productv1.DeleteProductRequest) (*productv1.DeleteProductResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid product id")
	}

	if err := h.store.DeleteProduct(ctx, id); err != nil {
		h.logger.Error("failed to delete product", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to delete product")
	}

	return &productv1.DeleteProductResponse{}, nil
}

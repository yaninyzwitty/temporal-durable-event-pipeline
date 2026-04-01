package handler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	userv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/user/v1"
)

type UserStore interface {
	CreateUser(ctx context.Context, username, email, passwordHash string) (repository.CreateUserRow, error)
	GetUserByID(ctx context.Context, id uuid.UUID) (repository.GetUserByIDRow, error)
	GetUserByEmail(ctx context.Context, email string) (repository.User, error)
	ListUsers(ctx context.Context) ([]repository.ListUsersRow, error)
	UpdateUser(ctx context.Context, id uuid.UUID, username, email string) (repository.UpdateUserRow, error)
	DeleteUser(ctx context.Context, id uuid.UUID) error
}

type UserHandler struct {
	userv1.UnimplementedUserServiceServer
	store  UserStore
	logger *slog.Logger
}

func NewUserHandler(store UserStore, logger *slog.Logger) *UserHandler {
	return &UserHandler{store: store, logger: logger}
}

func (h *UserHandler) CreateUser(ctx context.Context, req *userv1.CreateUserRequest) (*userv1.CreateUserResponse, error) {
	if req.GetUsername() == "" || req.GetEmail() == "" || req.GetPassword() == "" {
		return nil, status.Error(codes.InvalidArgument, "username, email, and password are required")
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.GetPassword()), bcrypt.DefaultCost)
	if err != nil {
		h.logger.Error("failed to hash password", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	row, err := h.store.CreateUser(ctx, req.GetUsername(), req.GetEmail(), string(hashedPassword))
	if err != nil {
		h.logger.Error("failed to create user", "error", err)
		return nil, status.Error(codes.Internal, "failed to create user")
	}

	return &userv1.CreateUserResponse{
		User: userRowToProto(row),
	}, nil
}

func (h *UserHandler) GetUser(ctx context.Context, req *userv1.GetUserRequest) (*userv1.GetUserResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}

	row, err := h.store.GetUserByID(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		h.logger.Error("failed to get user", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to get user")
	}

	return &userv1.GetUserResponse{
		User: getUserByIDRowToProto(row),
	}, nil
}

func (h *UserHandler) GetUserByEmail(ctx context.Context, req *userv1.GetUserByEmailRequest) (*userv1.GetUserByEmailResponse, error) {
	if req.GetEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "email is required")
	}

	user, err := h.store.GetUserByEmail(ctx, req.GetEmail())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		h.logger.Error("failed to get user by email", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}

	return &userv1.GetUserByEmailResponse{
		User: userModelToProto(user),
	}, nil
}

func (h *UserHandler) ListUsers(ctx context.Context, _ *userv1.ListUsersRequest) (*userv1.ListUsersResponse, error) {
	rows, err := h.store.ListUsers(ctx)
	if err != nil {
		h.logger.Error("failed to list users", "error", err)
		return nil, status.Error(codes.Internal, "failed to list users")
	}

	users := make([]*userv1.User, len(rows))
	for i, row := range rows {
		users[i] = listUsersRowToProto(row)
	}

	return &userv1.ListUsersResponse{Users: users}, nil
}

func (h *UserHandler) UpdateUser(ctx context.Context, req *userv1.UpdateUserRequest) (*userv1.UpdateUserResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}
	if req.GetUsername() == "" || req.GetEmail() == "" {
		return nil, status.Error(codes.InvalidArgument, "username and email are required")
	}

	row, err := h.store.UpdateUser(ctx, id, req.GetUsername(), req.GetEmail())
	if err != nil {
		h.logger.Error("failed to update user", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to update user")
	}

	return &userv1.UpdateUserResponse{
		User: updateUserRowToProto(row),
	}, nil
}

func (h *UserHandler) DeleteUser(ctx context.Context, req *userv1.DeleteUserRequest) (*userv1.DeleteUserResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid user id")
	}

	if err := h.store.DeleteUser(ctx, id); err != nil {
		h.logger.Error("failed to delete user", "error", err, "id", req.GetId())
		return nil, status.Error(codes.Internal, "failed to delete user")
	}

	return &userv1.DeleteUserResponse{}, nil
}

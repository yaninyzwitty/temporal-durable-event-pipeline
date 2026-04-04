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

	userv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/user/v1"
)

func setupUserHandler(t *testing.T) (*MockUserStore, *UserHandler) {
	ctrl := gomock.NewController(t)
	mockStore := NewMockUserStore(ctrl)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := NewUserHandler(mockStore, logger)
	return mockStore, handler
}

func TestUserHandler_CreateUser_Success(t *testing.T) {
	mockStore, handler := setupUserHandler(t)
	ctx := context.Background()

	req := &userv1.CreateUserRequest{
		Username: "testuser",
		Email:    "test@example.com",
		Password: "password123",
	}

	expectedRow := repository.CreateUserRow{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Username: "testuser",
		Email:    "test@example.com",
	}

	mockStore.EXPECT().CreateUser(gomock.Any(), "testuser", "test@example.com", gomock.Any()).Return(expectedRow, nil)

	resp, err := handler.CreateUser(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.User == nil {
		t.Fatal("expected user to not be nil")
	}
	if resp.User.Username != "testuser" {
		t.Errorf("expected username 'testuser', got %s", resp.User.Username)
	}
}

func TestUserHandler_CreateUser_MissingFields(t *testing.T) {
	_, handler := setupUserHandler(t)
	ctx := context.Background()

	tests := []struct {
		name string
		req  *userv1.CreateUserRequest
	}{
		{"missing username", &userv1.CreateUserRequest{Email: "test@example.com", Password: "pass"}},
		{"missing email", &userv1.CreateUserRequest{Username: "testuser", Password: "pass"}},
		{"missing password", &userv1.CreateUserRequest{Username: "testuser", Email: "test@example.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler.CreateUser(ctx, tt.req)
			s, _ := status.FromError(err)
			if s.Code() != codes.InvalidArgument {
				t.Errorf("expected InvalidArgument, got %v", s.Code())
			}
		})
	}
}

func TestUserHandler_GetUser_Success(t *testing.T) {
	mockStore, handler := setupUserHandler(t)
	ctx := context.Background()

	userID := uuid.New()
	req := &userv1.GetUserRequest{Id: userID.String()}

	expectedRow := repository.GetUserByIDRow{
		ID:       pgtype.UUID{Bytes: userID, Valid: true},
		Username: "testuser",
		Email:    "test@example.com",
	}

	mockStore.EXPECT().GetUserByID(gomock.Any(), userID).Return(expectedRow, nil)

	resp, err := handler.GetUser(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.User == nil {
		t.Fatal("expected user to not be nil")
	}
	if resp.User.Username != "testuser" {
		t.Errorf("expected username 'testuser', got %s", resp.User.Username)
	}
}

func TestUserHandler_GetUser_InvalidID(t *testing.T) {
	_, handler := setupUserHandler(t)
	ctx := context.Background()

	req := &userv1.GetUserRequest{Id: "invalid-uuid"}

	_, err := handler.GetUser(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestUserHandler_GetUser_NotFound(t *testing.T) {
	mockStore, handler := setupUserHandler(t)
	ctx := context.Background()

	userID := uuid.New()
	req := &userv1.GetUserRequest{Id: userID.String()}

	mockStore.EXPECT().GetUserByID(gomock.Any(), userID).Return(repository.GetUserByIDRow{}, pgx.ErrNoRows)

	_, err := handler.GetUser(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got %v", s.Code())
	}
}

func TestUserHandler_GetUserByEmail_Success(t *testing.T) {
	mockStore, handler := setupUserHandler(t)
	ctx := context.Background()

	req := &userv1.GetUserByEmailRequest{Email: "test@example.com"}

	expectedUser := repository.User{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		Username: "testuser",
		Email:    "test@example.com",
	}

	mockStore.EXPECT().GetUserByEmail(gomock.Any(), "test@example.com").Return(expectedUser, nil)

	resp, err := handler.GetUserByEmail(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.User == nil {
		t.Fatal("expected user to not be nil")
	}
}

func TestUserHandler_GetUserByEmail_MissingEmail(t *testing.T) {
	_, handler := setupUserHandler(t)
	ctx := context.Background()

	req := &userv1.GetUserByEmailRequest{Email: ""}

	_, err := handler.GetUserByEmail(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestUserHandler_ListUsers_Success(t *testing.T) {
	mockStore, handler := setupUserHandler(t)
	ctx := context.Background()

	users := []repository.ListUsersRow{
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, Username: "user1", Email: "user1@example.com"},
		{ID: pgtype.UUID{Bytes: uuid.New(), Valid: true}, Username: "user2", Email: "user2@example.com"},
	}

	mockStore.EXPECT().ListUsers(gomock.Any()).Return(users, nil)

	resp, err := handler.ListUsers(ctx, &userv1.ListUsersRequest{})

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(resp.Users) != 2 {
		t.Errorf("expected 2 users, got %d", len(resp.Users))
	}
}

func TestUserHandler_UpdateUser_Success(t *testing.T) {
	mockStore, handler := setupUserHandler(t)
	ctx := context.Background()

	userID := uuid.New()
	req := &userv1.UpdateUserRequest{
		Id:       userID.String(),
		Username: "updateduser",
		Email:    "updated@example.com",
	}

	expectedRow := repository.UpdateUserRow{
		ID:       pgtype.UUID{Bytes: userID, Valid: true},
		Username: "updateduser",
		Email:    "updated@example.com",
	}

	mockStore.EXPECT().UpdateUser(gomock.Any(), userID, "updateduser", "updated@example.com").Return(expectedRow, nil)

	resp, err := handler.UpdateUser(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if resp.User.Username != "updateduser" {
		t.Errorf("expected username 'updateduser', got %s", resp.User.Username)
	}
}

func TestUserHandler_UpdateUser_InvalidID(t *testing.T) {
	_, handler := setupUserHandler(t)
	ctx := context.Background()

	req := &userv1.UpdateUserRequest{
		Id:       "invalid-id",
		Username: "updateduser",
		Email:    "updated@example.com",
	}

	_, err := handler.UpdateUser(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

func TestUserHandler_DeleteUser_Success(t *testing.T) {
	mockStore, handler := setupUserHandler(t)
	ctx := context.Background()

	userID := uuid.New()
	req := &userv1.DeleteUserRequest{Id: userID.String()}

	mockStore.EXPECT().DeleteUser(gomock.Any(), userID).Return(nil)

	_, err := handler.DeleteUser(ctx, req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestUserHandler_DeleteUser_InvalidID(t *testing.T) {
	_, handler := setupUserHandler(t)
	ctx := context.Background()

	req := &userv1.DeleteUserRequest{Id: "invalid-id"}

	_, err := handler.DeleteUser(ctx, req)

	s, _ := status.FromError(err)
	if s.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", s.Code())
	}
}

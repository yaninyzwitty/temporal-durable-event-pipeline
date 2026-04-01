package server

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/handler"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	eventv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/event/v1"
	orderv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/order/v1"
	productv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/product/v1"
	userv1 "github.com/yaninyzwitty/temporal-durable-event-pipeline/gen/user/v1"
)

type Server struct {
	grpcServer *grpc.Server
	port       int
	logger     *slog.Logger
}

func New(port int, store *repository.Store, logger *slog.Logger, opts ...grpc.ServerOption) *Server {
	grpcServer := grpc.NewServer(opts...)

	userv1.RegisterUserServiceServer(grpcServer, handler.NewUserHandler(store, logger))
	productv1.RegisterProductServiceServer(grpcServer, handler.NewProductHandler(store, logger))
	orderv1.RegisterOrderServiceServer(grpcServer, handler.NewOrderHandler(store, logger))
	eventv1.RegisterEventServiceServer(grpcServer, handler.NewEventHandler(store, logger))

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	reflection.Register(grpcServer)

	return &Server{
		grpcServer: grpcServer,
		port:       port,
		logger:     logger,
	}
}

func (s *Server) Start() (net.Listener, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", s.port, err)
	}

	s.logger.Info("gRPC server starting", "port", s.port)
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Error("gRPC server failed", "error", err)
		}
	}()

	return lis, nil
}

func (s *Server) GracefulStop() {
	s.logger.Info("shutting down gRPC server gracefully")
	s.grpcServer.GracefulStop()
}

func (s *Server) Stop() {
	s.logger.Info("stopping gRPC server")
	s.grpcServer.Stop()
}

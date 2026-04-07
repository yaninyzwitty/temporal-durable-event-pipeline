package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/handler"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/poller"
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
	grpcServer     *grpc.Server
	port           int
	logger         *slog.Logger
	outboxListener *poller.OutboxListener
	outboxCancel   context.CancelFunc
}

func New(port int, store *repository.Store, env string, logger *slog.Logger, opts ...grpc.ServerOption) *Server {
	grpcServer := grpc.NewServer(opts...)

	userv1.RegisterUserServiceServer(grpcServer, handler.NewUserHandler(store, logger))
	productv1.RegisterProductServiceServer(grpcServer, handler.NewProductHandler(store, logger))
	orderv1.RegisterOrderServiceServer(grpcServer, handler.NewOrderHandler(store, logger))
	eventv1.RegisterEventServiceServer(grpcServer, handler.NewEventHandler(store, logger))

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	healthServer.SetServingStatus(userv1.UserService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(productv1.ProductService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(orderv1.OrderService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(eventv1.EventService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)

	if env == "development" {
		reflection.Register(grpcServer)
		logger.Info("gRPC reflection enabled")
	}

	return &Server{
		grpcServer: grpcServer,
		port:       port,
		logger:     logger,
	}
}

func (s *Server) SetOutboxListener(listener *poller.OutboxListener) {
	s.outboxListener = listener
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

	if s.outboxListener != nil {
		s.logger.Info("starting outbox listener")
		ctx, cancel := context.WithCancel(context.Background())
		s.outboxCancel = cancel
		if err := s.outboxListener.Start(ctx); err != nil {
			s.logger.Error("failed to start outbox listener", "error", err)
			cancel()
			s.outboxCancel = nil
		}
	}

	return lis, nil
}

func (s *Server) GracefulStop() {
	s.logger.Info("shutting down gRPC server gracefully")
	if s.outboxCancel != nil {
		s.logger.Info("canceling outbox listener context")
		s.outboxCancel()
	}
	if s.outboxListener != nil {
		s.logger.Info("stopping outbox listener")
		s.outboxListener.Stop()
	}
	s.grpcServer.GracefulStop()
}

func (s *Server) Stop() {
	s.logger.Info("stopping gRPC server")
	if s.outboxCancel != nil {
		s.logger.Info("canceling outbox listener context")
		s.outboxCancel()
	}
	if s.outboxListener != nil {
		s.logger.Info("stopping outbox listener")
		s.outboxListener.Stop()
	}
	s.grpcServer.Stop()
}

package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yaninyzwitty/temporal-durable-event-pipeline/db"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/repository"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/internal/server"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/shared/logger"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/shared/pkg"
)

const (
	MAX_RETRIES int = 10
)

func main() {
	baseCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log := logger.New(logger.EnvDevelopment)

	configPath := flag.String("config", getEnvOrDefault("SERVER_CONFIG", "config.yaml"), "path to the config file")
	flag.Parse()

	var config pkg.Config
	if err := config.Load(log, *configPath); err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	dbConfig := pkg.DatabaseConfig{
		Host:     config.DatabaseConfig.Host,
		Port:     config.DatabaseConfig.Port,
		User:     config.DatabaseConfig.User,
		Password: config.DatabaseConfig.Password,
		Name:     config.DatabaseConfig.Name,
		SSLMode:  config.DatabaseConfig.SSLMode,
	}

	pool, err := db.WaitForDB(baseCtx, dbConfig, MAX_RETRIES, db.DefaultPoolOptions()...)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	store := repository.NewStore(pool)
	srv := server.New(config.ServerConfig.Port, store, log)

	lis, err := srv.Start()
	if err != nil {
		log.Error("failed to start gRPC server", "error", err)
		os.Exit(1)
	}
	log.Info("server started", "address", lis.Addr().String())

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("received shutdown signal")
	srv.GracefulStop()
	log.Info("server stopped")
}

func getEnvOrDefault(envKey, defaultValue string) string {
	if value, exists := os.LookupEnv(envKey); exists {
		return value
	}
	return defaultValue
}

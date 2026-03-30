package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/yaninyzwitty/temporal-durable-event-pipeline/db"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/shared/logger"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/shared/pkg"
)

const (
	MAX_RETRIES = 10
)

func main() {

	// Create a context with timeout for database connection
	baseCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Initialize logger with a default environment
	log := logger.New(logger.EnvDevelopment)

	configPath := flag.String("config", getEnvOrDefault("SERVER_CONFIG", "config.yaml"), "path to the config file")

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

	// Initialize database connection pool with default options
	pool, err := db.WaitForDB(baseCtx, dbConfig, MAX_RETRIES, db.DefaultPoolOptions()...)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	log.Info("successfully connected to database")

}

// getEnvOrDefault checks if an environment variable exists and returns its value, otherwise it returns a default value.
func getEnvOrDefault(envKey, defaultValue string) string {
	if value, exists := os.LookupEnv(envKey); exists {
		return value
	}
	return defaultValue
}

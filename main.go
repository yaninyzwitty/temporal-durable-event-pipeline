package main

import (
	"os"

	"github.com/yaninyzwitty/temporal-durable-event-pipeline/shared/logger"
	"github.com/yaninyzwitty/temporal-durable-event-pipeline/shared/pkg"
)

func main() {
	// Initialize logger with a default environment
	log := logger.New(logger.EnvDevelopment)

	var config pkg.Config
	if err := config.Load(log, "config.yaml"); err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Use the loaded configuration
	log.Info("Starting server", "port", config.ServerConfig.Port, "env", config.ServerConfig.Env)
}

package pkg

import (
	"fmt"
	"log/slog"
	"os"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	ServerConfig ServerConfig `yaml:"server"`
}

type ServerConfig struct {
	Port int    `yaml:"port"`
	Env  string `yaml:"env"`
}

func (c *Config) Load(logger *slog.Logger, path string) error {
	// read file
	file, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file, %w", err)
	}

	if err := yaml.Unmarshal(file, c); err != nil {
		return fmt.Errorf("failed to unmarshal config file, %w", err)

	}

	logger.Info("config loaded successfully")
	return nil
}

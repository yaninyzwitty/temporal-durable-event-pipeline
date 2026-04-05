package pkg

import (
	"fmt"
	"log/slog"
	"os"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	ServerConfig   ServerConfig   `yaml:"server"`
	DatabaseConfig DatabaseConfig `yaml:"database"`
	RedpandaConfig RedpandaConfig `yaml:"redpanda"`
}

type ServerConfig struct {
	Port int    `yaml:"port"`
	Env  string `yaml:"env"`
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	SSLMode  string `yaml:"sslMode"`
	Name     string `yaml:"name"`
	Password string `yaml:"password"`
}

type RedpandaConfig struct {
	Brokers     []string `yaml:"brokers"`
	TopicPrefix string   `yaml:"topicPrefix"`
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

	if len(c.RedpandaConfig.Brokers) == 0 {
		return fmt.Errorf("redpanda.brokers must not be empty")
	}

	if c.RedpandaConfig.TopicPrefix == "" {
		return fmt.Errorf("redpanda.topicPrefix must not be empty")
	}

	if c.DatabaseConfig.Password == "" || c.DatabaseConfig.Password == "${DB_PASSWORD}" {
		c.DatabaseConfig.Password = os.Getenv("DB_PASSWORD")
	}

	logger.Info("config loaded successfully", "server_port", c.ServerConfig.Port)
	return nil
}

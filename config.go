package main

import (
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type DatabaseConfig struct {
	ConnStr         string   `yaml:"conn_str"`
	SlotName        string   `yaml:"slot_name"`
	PublicationName string   `yaml:"publication_name"`
	Tables          []string `yaml:"tables"`
}

type Config struct {
	ConnectorName string           `yaml:"connector_name"`
	FlushInterval time.Duration    `yaml:"flush_interval"`
	OffsetFile    string           `yaml:"offset_file"`
	Databases     []DatabaseConfig `yaml:"databases"`
	RetryConfig   RetryConfig      `yaml:"retry_config"`
}

// RetryConfig defines retry behavior for various operations
type RetryConfig struct {
	MaxRetries        int           `yaml:"max_retries"`
	InitialBackoff    time.Duration `yaml:"initial_backoff"`
	MaxBackoff        time.Duration `yaml:"max_backoff"`
	BackoffMultiplier float64       `yaml:"backoff_multiplier"`
}

// getEnv gets an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// LoadConfig loads configuration from config.yaml
func LoadConfig() *Config {
	// Read config file
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	// Parse YAML
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// Set defaults if not specified
	if cfg.ConnectorName == "" {
		cfg.ConnectorName = "go-cdc"
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 10 * time.Second
	}
	if cfg.OffsetFile == "" {
		cfg.OffsetFile = "offset.json"
	}

	// Set retry defaults
	if cfg.RetryConfig.MaxRetries == 0 {
		cfg.RetryConfig.MaxRetries = 3
	}
	if cfg.RetryConfig.InitialBackoff == 0 {
		cfg.RetryConfig.InitialBackoff = 1 * time.Second
	}
	if cfg.RetryConfig.MaxBackoff == 0 {
		cfg.RetryConfig.MaxBackoff = 30 * time.Second
	}
	if cfg.RetryConfig.BackoffMultiplier == 0 {
		cfg.RetryConfig.BackoffMultiplier = 2.0
	}

	return &cfg
}

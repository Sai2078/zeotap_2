package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the application
type Config struct {
	// Server configuration
	ServerAddr    string
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	AllowedOrigin string

	// ClickHouse configuration
	DefaultClickHousePort int
	DefaultHTTPPort       int

	// Batch settings
	BatchSize          int
	ProgressReportSize int
	MaxPreviewRows     int
}

// Load loads configuration from environment variables with defaults
func Load() (*Config, error) {
	cfg := &Config{
		ServerAddr:          getEnv("SERVER_ADDR", ":8080"),
		ReadTimeout:         getEnvDuration("READ_TIMEOUT", 30*time.Second),
		WriteTimeout:        getEnvDuration("WRITE_TIMEOUT", 30*time.Second),
		AllowedOrigin:       getEnv("ALLOWED_ORIGIN", "*"),
		DefaultClickHousePort: getEnvInt("DEFAULT_CLICKHOUSE_PORT", 9000),
		DefaultHTTPPort:     getEnvInt("DEFAULT_HTTP_PORT", 8123),
		BatchSize:           getEnvInt("BATCH_SIZE", 10000),
		ProgressReportSize:  getEnvInt("PROGRESS_REPORT_SIZE", 5000),
		MaxPreviewRows:      getEnvInt("MAX_PREVIEW_ROWS", 100),
	}

	return cfg, nil
}

// Helper functions to get environment variables with defaults
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		intVal, err := strconv.Atoi(value)
		if err != nil {
			return fallback
		}
		return intVal
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if value, exists := os.LookupEnv(key); exists {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return fallback
		}
		return duration
	}
	return fallback
}
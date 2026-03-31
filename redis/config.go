package redis

import (
	"errors"
	"fmt"
	"strings"
)

// Config configures Redis persistence
type Config struct {
	Addr     string
	Password string
	Prefix   string
	Shard    string
	DB       int
}

const (
	// DefaultEndpoint is the default Redis/Valkey endpoint
	DefaultEndpoint = "127.0.0.1:6379"

	// DefaultPrefix is the default key prefix
	DefaultPrefix = "timebox"

	// DefaultDB is the default Redis/Valkey database
	DefaultDB = 0
)

var (
	// ErrInvalidDB indicates DB is below the allowed range
	ErrInvalidDB = errors.New("db must be >= 0")

	// ErrInvalidShard indicates Shard contains disallowed characters
	ErrInvalidShard = errors.New("shard cannot contain braces or colons")
)

// DefaultConfig returns a Config populated with sensible defaults
func DefaultConfig() Config {
	return Config{
		Addr:   DefaultEndpoint,
		Prefix: DefaultPrefix,
		DB:     DefaultDB,
	}
}

// With overlays the non-zero values from other onto cfg
func (cfg Config) With(other Config) Config {
	if other.Addr != "" {
		cfg.Addr = other.Addr
	}
	if other.Password != "" {
		cfg.Password = other.Password
	}
	if other.Prefix != "" {
		cfg.Prefix = other.Prefix
	}
	if other.Shard != "" {
		cfg.Shard = other.Shard
	}
	if other.DB != 0 {
		cfg.DB = other.DB
	}
	return cfg
}

// Validate reports whether the configuration contains invalid values
func (cfg Config) Validate() error {
	switch {
	case cfg.DB < 0:
		return ErrInvalidDB
	case strings.ContainsAny(cfg.Shard, "{}:"):
		return ErrInvalidShard
	}
	return nil
}

func buildStorePrefix(cfg Config) string {
	if cfg.Shard == "" {
		return cfg.Prefix
	}
	if cfg.Prefix == "" {
		return "{" + cfg.Shard + "}"
	}
	return fmt.Sprintf("%s:{%s}", cfg.Prefix, cfg.Shard)
}

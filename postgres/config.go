package postgres

import (
	"errors"

	"github.com/kode4food/timebox"
)

type (
	// Config configures Postgres persistence and embeds Timebox store behavior
	Config struct {
		Timebox  timebox.Config
		URL      string
		Prefix   string
		MaxConns int32
	}
)

const (
	// DefaultURL is the default Postgres connection URL
	DefaultURL = "postgres://localhost:5432/postgres?sslmode=disable"

	// DefaultPrefix is the default logical store namespace
	DefaultPrefix = "timebox"

	// DefaultMaxConns is the default pgx pool size
	DefaultMaxConns = 96
)

var (
	// ErrURLRequired indicates URL must be provided
	ErrURLRequired = errors.New("postgres URL is required")

	// ErrPrefixRequired indicates Prefix must be provided
	ErrPrefixRequired = errors.New("prefix is required")

	// ErrInvalidMaxConns indicates MaxConns must be positive
	ErrInvalidMaxConns = errors.New("max conns must be positive")
)

// DefaultConfig returns a Config populated with sensible defaults
func DefaultConfig() Config {
	return Config{
		Timebox:  timebox.DefaultConfig(),
		URL:      DefaultURL,
		Prefix:   DefaultPrefix,
		MaxConns: DefaultMaxConns,
	}
}

// With overlays the non-zero values from other onto cfg
func (cfg Config) With(other Config) Config {
	cfg.Timebox = timebox.Configure(cfg.Timebox, other.Timebox)
	if other.URL != "" {
		cfg.URL = other.URL
	}
	if other.Prefix != "" {
		cfg.Prefix = other.Prefix
	}
	if other.MaxConns != 0 {
		cfg.MaxConns = other.MaxConns
	}
	return cfg
}

// Validate reports whether the configuration contains invalid values
func (cfg Config) Validate() error {
	switch {
	case cfg.URL == "":
		return ErrURLRequired
	case cfg.Prefix == "":
		return ErrPrefixRequired
	case cfg.MaxConns <= 0:
		return ErrInvalidMaxConns
	}
	return cfg.Timebox.Validate()
}

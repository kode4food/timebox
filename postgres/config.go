package postgres

import "errors"

// Config configures a Postgres Backend
type Config struct {
	URL      string
	Prefix   string
	MaxConns int32
}

const (
	// DefaultURL is the default Postgres connection URL
	DefaultURL = "postgres://localhost:5432/postgres?sslmode=disable"

	// DefaultPrefix is the default logical store namespace
	DefaultPrefix = "timebox"

	// DefaultMaxConns is the default pgx pool size
	DefaultMaxConns = 32
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
		URL:      DefaultURL,
		Prefix:   DefaultPrefix,
		MaxConns: DefaultMaxConns,
	}
}

// With overlays the non-zero values from other onto cfg
func (c Config) With(other Config) Config {
	if other.URL != "" {
		c.URL = other.URL
	}
	if other.Prefix != "" {
		c.Prefix = other.Prefix
	}
	if other.MaxConns != 0 {
		c.MaxConns = other.MaxConns
	}
	return c
}

// Validate reports whether the configuration contains invalid values
func (c Config) Validate() error {
	switch {
	case c.URL == "":
		return ErrURLRequired
	case c.Prefix == "":
		return ErrPrefixRequired
	case c.MaxConns <= 0:
		return ErrInvalidMaxConns
	}
	return nil
}

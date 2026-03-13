package redis

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kode4food/timebox"
)

type (
	// Config configures Redis persistence and embeds Timebox store behavior
	Config struct {
		Timebox  timebox.Config
		Addr     string
		Password string
		Prefix   string
		Shard    string
		DB       int
		JoinKey  JoinKeyFunc
		ParseKey ParseKeyFunc
	}

	// JoinKeyFunc joins an aggregate ID into the identity portion of a Redis
	// key
	JoinKeyFunc func(timebox.AggregateID) string

	// ParseKeyFunc reconstructs an aggregate ID from the identity portion of a
	// Redis key
	ParseKeyFunc func(string) timebox.AggregateID
)

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

	// ErrJoinKeyRequired indicates JoinKey must be provided
	ErrJoinKeyRequired = errors.New("join key function is required")

	// ErrParseKeyRequired indicates ParseKey must be provided
	ErrParseKeyRequired = errors.New("parse key function is required")
)

// DefaultConfig returns a Config populated with sensible defaults
func DefaultConfig() Config {
	return Config{
		Timebox:  timebox.DefaultConfig(),
		Addr:     DefaultEndpoint,
		Prefix:   DefaultPrefix,
		DB:       DefaultDB,
		JoinKey:  JoinKey,
		ParseKey: ParseKey,
	}
}

// With overlays the non-zero values from other onto cfg
func (cfg Config) With(other Config) Config {
	cfg.Timebox = timebox.Configure(cfg.Timebox, other.Timebox)
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
	if other.JoinKey != nil {
		cfg.JoinKey = other.JoinKey
	}
	if other.ParseKey != nil {
		cfg.ParseKey = other.ParseKey
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
	case cfg.JoinKey == nil:
		return ErrJoinKeyRequired
	case cfg.ParseKey == nil:
		return ErrParseKeyRequired
	}
	return cfg.Timebox.Validate()
}

// JoinKey is the default JoinKeyFunc; it joins AggregateID parts with ":"
func JoinKey(id timebox.AggregateID) string {
	return id.Join(":")
}

// ParseKey is the default ParseKeyFunc; it splits on ":" to reconstruct an
// AggregateID
func ParseKey(str string) timebox.AggregateID {
	return timebox.ParseAggregateID(str, ":")
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

package timebox

import (
	"errors"
	"strings"
	"time"
)

type (
	// Config configures Store behavior
	Config struct {
		Redis      RedisConfig
		Snapshot   SnapshotConfig
		MaxRetries int
		CacheSize  int
		Archiving  bool
		Indexer    Indexer
	}

	// SnapshotConfig configures snapshot workers and event trimming
	SnapshotConfig struct {
		Workers      bool
		WorkerCount  int
		MaxQueueSize int
		SaveTimeout  time.Duration
		TrimEvents   bool
	}

	// RedisConfig configures the Redis/Valkey-specific backing store details
	RedisConfig struct {
		Addr     string
		Password string
		Prefix   string
		Shard    string
		DB       int
		JoinKey  JoinKeyFunc
		ParseKey ParseKeyFunc
	}
)

const (
	// DefaultRedisEndpoint is the default Redis host:port
	DefaultRedisEndpoint = "localhost:6379"

	// DefaultRedisPrefix prefixes all keys written by the store
	DefaultRedisPrefix = "timebox"

	// DefaultRedisDB is the default Redis database index
	DefaultRedisDB = 0

	// DefaultWorkers determines support for background snapshot workers
	DefaultWorkers = false

	// DefaultSnapshotWorkers is the number of background snapshot workers
	DefaultSnapshotWorkers = 4

	// DefaultSnapshotQueueSize is the snapshot worker channel capacity
	DefaultSnapshotQueueSize = 1024

	// DefaultSnapshotSaveTimeout is the timeout for snapshot persistence
	DefaultSnapshotSaveTimeout = 30 * time.Second

	// DefaultTrimEvents determines whether snapshots trim stored events
	DefaultTrimEvents = false

	// DefaultMaxRetries is the default optimistic concurrency retry count
	DefaultMaxRetries = 16

	// DefaultCacheSize controls the projection LRU size
	DefaultCacheSize = 128
)

var (
	// ErrInvalidMaxRetries indicates MaxRetries is below the allowed range
	ErrInvalidMaxRetries = errors.New("max retries must be > 0")

	// ErrInvalidCacheSize indicates CacheSize is below the allowed range
	ErrInvalidCacheSize = errors.New("cache size must be > 0")

	// ErrInvalidRedisDB indicates DB is below the allowed range
	ErrInvalidRedisDB = errors.New("db must be >= 0")

	// ErrInvalidWorkerCount indicates WorkerCount is below the allowed range
	ErrInvalidWorkerCount = errors.New("worker count must be > 0")

	// ErrInvalidMaxQueueSize indicates MaxQueueSize is below the allowed range
	ErrInvalidMaxQueueSize = errors.New("max queue size must be > 0")

	// ErrInvalidSaveTimeout indicates SaveTimeout is below the allowed range
	ErrInvalidSaveTimeout = errors.New("save timeout must be > 0")

	// ErrInvalidShard indicates Shard contains invalid brace characters
	ErrInvalidShard = errors.New("shard must not contain braces")

	// ErrJoinKeyRequired indicates JoinKey must be provided
	ErrJoinKeyRequired = errors.New("join key function is required")

	// ErrParseKeyRequired indicates ParseKey must be provided
	ErrParseKeyRequired = errors.New("parse key function is required")
)

// DefaultConfig returns a Config populated with sensible defaults
func DefaultConfig() Config {
	return Config{
		Redis: RedisConfig{
			Addr:     DefaultRedisEndpoint,
			Prefix:   DefaultRedisPrefix,
			DB:       DefaultRedisDB,
			JoinKey:  JoinKey,
			ParseKey: ParseKey,
		},
		Snapshot: SnapshotConfig{
			Workers:      DefaultWorkers,
			WorkerCount:  DefaultSnapshotWorkers,
			MaxQueueSize: DefaultSnapshotQueueSize,
			SaveTimeout:  DefaultSnapshotSaveTimeout,
			TrimEvents:   DefaultTrimEvents,
		},
		MaxRetries: DefaultMaxRetries,
		CacheSize:  DefaultCacheSize,
	}
}

// With overlays the non-zero values from others onto cfg
func (cfg Config) With(others ...Config) Config {
	for _, other := range others {
		cfg.Redis = cfg.Redis.With(other.Redis)
		cfg.Snapshot = cfg.Snapshot.With(other.Snapshot)
		if other.MaxRetries != 0 {
			cfg.MaxRetries = other.MaxRetries
		}
		if other.CacheSize != 0 {
			cfg.CacheSize = other.CacheSize
		}
		if other.Archiving {
			cfg.Archiving = other.Archiving
		}
		if other.Indexer != nil {
			cfg.Indexer = other.Indexer
		}
	}
	return cfg
}

// Validate reports whether the configuration contains invalid values
func (cfg Config) Validate() error {
	switch {
	case cfg.MaxRetries <= 0:
		return ErrInvalidMaxRetries
	case cfg.CacheSize <= 0:
		return ErrInvalidCacheSize
	}
	if err := cfg.Snapshot.Validate(); err != nil {
		return err
	}
	return cfg.Redis.Validate()
}

// With overlays the non-zero values from other onto cfg
func (cfg SnapshotConfig) With(other SnapshotConfig) SnapshotConfig {
	if other.Workers {
		cfg.Workers = other.Workers
	}
	if other.WorkerCount != 0 {
		cfg.WorkerCount = other.WorkerCount
	}
	if other.MaxQueueSize != 0 {
		cfg.MaxQueueSize = other.MaxQueueSize
	}
	if other.SaveTimeout != 0 {
		cfg.SaveTimeout = other.SaveTimeout
	}
	if other.TrimEvents {
		cfg.TrimEvents = other.TrimEvents
	}
	return cfg
}

// Validate reports whether the snapshot configuration contains invalid values
func (cfg SnapshotConfig) Validate() error {
	switch {
	case cfg.WorkerCount <= 0:
		return ErrInvalidWorkerCount
	case cfg.MaxQueueSize <= 0:
		return ErrInvalidMaxQueueSize
	case cfg.SaveTimeout <= 0:
		return ErrInvalidSaveTimeout
	}
	return nil
}

// With overlays the non-zero values from other onto cfg
func (cfg RedisConfig) With(other RedisConfig) RedisConfig {
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

// Validate reports whether the Redis configuration contains invalid values
func (cfg RedisConfig) Validate() error {
	switch {
	case cfg.DB < 0:
		return ErrInvalidRedisDB
	case cfg.JoinKey == nil:
		return ErrJoinKeyRequired
	case cfg.ParseKey == nil:
		return ErrParseKeyRequired
	case strings.ContainsAny(cfg.Shard, "{}"):
		return ErrInvalidShard
	}
	return nil
}

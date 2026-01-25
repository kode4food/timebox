package timebox

import "time"

type (
	// Config controls Timebox behavior, including store settings and tuning
	// knobs for retries, caching, and snapshot workers
	Config struct {
		Store      StoreConfig
		MaxRetries int
		CacheSize  int
		Workers    bool
	}

	// StoreConfig configures the Redis/Valkey store backing the event log and
	// snapshots
	StoreConfig struct {
		Addr         string
		Password     string
		Prefix       string
		DB           int
		WorkerCount  int
		MaxQueueSize int
		SaveTimeout  time.Duration
		TrimEvents   bool
		Archiving    bool
	}
)

const (
	// DefaultRedisEndpoint is the default Redis host:port
	DefaultRedisEndpoint = "localhost:6379"

	// DefaultRedisPrefix prefixes all keys written by the store
	DefaultRedisPrefix = "timebox"

	// DefaultRedisDB is the default Redis database index
	DefaultRedisDB = 0

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

	// DefaultExecutorCacheSize controls the projection LRU size
	DefaultExecutorCacheSize = 128
)

// DefaultConfig returns a Config populated with sensible defaults
func DefaultConfig() Config {
	return Config{
		Store:      DefaultStoreConfig(),
		MaxRetries: DefaultMaxRetries,
		CacheSize:  DefaultExecutorCacheSize,
		Workers:    true,
	}
}

// DefaultStoreConfig returns a StoreConfig with default Redis settings
func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		Addr:         DefaultRedisEndpoint,
		DB:           DefaultRedisDB,
		Prefix:       DefaultRedisPrefix,
		WorkerCount:  DefaultSnapshotWorkers,
		MaxQueueSize: DefaultSnapshotQueueSize,
		SaveTimeout:  DefaultSnapshotSaveTimeout,
		TrimEvents:   DefaultTrimEvents,
	}
}

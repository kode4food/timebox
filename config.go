package timebox

import "errors"

type (
	// Config configures Store behavior
	Config struct {
		Snapshot   SnapshotConfig
		MaxRetries int
		CacheSize  int
		Indexer    Indexer
	}

	// SnapshotConfig configures snapshot workers and event trimming
	SnapshotConfig struct {
		Workers      bool
		WorkerCount  int
		MaxQueueSize int
		TrimEvents   bool
	}

	// With is a generic overlay contract used by Configure
	With[T any] interface {
		With(T) T
	}
)

const (
	// DefaultWorkers determines support for background snapshot workers
	DefaultWorkers = false

	// DefaultSnapshotWorkers is the number of background snapshot workers
	DefaultSnapshotWorkers = 4

	// DefaultSnapshotQueueSize is the snapshot worker channel capacity
	DefaultSnapshotQueueSize = 1024

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

	// ErrInvalidWorkerCount indicates WorkerCount is below the allowed range
	ErrInvalidWorkerCount = errors.New("worker count must be > 0")

	// ErrInvalidMaxQueueSize indicates MaxQueueSize is below the allowed range
	ErrInvalidMaxQueueSize = errors.New("max queue size must be > 0")
)

// Configure overlays each supplied value on top of defaults in order
func Configure[T With[T]](defaults T, others ...T) T {
	for _, other := range others {
		defaults = defaults.With(other)
	}
	return defaults
}

// DefaultConfig returns a Config populated with sensible defaults
func DefaultConfig() Config {
	return Config{
		Snapshot: SnapshotConfig{
			Workers:      DefaultWorkers,
			WorkerCount:  DefaultSnapshotWorkers,
			MaxQueueSize: DefaultSnapshotQueueSize,
			TrimEvents:   DefaultTrimEvents,
		},
		MaxRetries: DefaultMaxRetries,
		CacheSize:  DefaultCacheSize,
	}
}

// With overlays the non-zero values from other onto cfg
func (cfg Config) With(other Config) Config {
	cfg.Snapshot = cfg.Snapshot.With(other.Snapshot)
	if other.MaxRetries != 0 {
		cfg.MaxRetries = other.MaxRetries
	}
	if other.CacheSize != 0 {
		cfg.CacheSize = other.CacheSize
	}
	if other.Indexer != nil {
		cfg.Indexer = other.Indexer
	}
	return cfg
}

// Validate reports whether the configuration contains invalid values
func (cfg Config) Validate() error {
	return cfg.validateStore()
}

func (cfg Config) validateStore() error {
	switch {
	case cfg.MaxRetries <= 0:
		return ErrInvalidMaxRetries
	case cfg.CacheSize <= 0:
		return ErrInvalidCacheSize
	}
	if err := cfg.Snapshot.Validate(); err != nil {
		return err
	}
	return nil
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
	}
	return nil
}

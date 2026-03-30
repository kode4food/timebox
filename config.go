package timebox

import "errors"

type (
	// Config configures Store behavior
	Config struct {
		TrimEvents    bool
		SnapshotRatio float64
		MaxRetries    int
		CacheSize     int
		Indexer       Indexer
	}

	// With is a generic overlay contract used by Configure
	With[T any] interface {
		With(T) T
	}
)

const (
	// DefaultTrimEvents determines whether snapshots trim stored events
	DefaultTrimEvents = false

	// DefaultSnapshotRatio is the default EventsSize/SnapshotSize threshold
	DefaultSnapshotRatio = 1.0

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

	// ErrInvalidSnapshotRatio indicates SnapshotRatio is below the allowed range
	ErrInvalidSnapshotRatio = errors.New("snapshot ratio must be >= 0")
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
		TrimEvents:    DefaultTrimEvents,
		SnapshotRatio: DefaultSnapshotRatio,
		MaxRetries:    DefaultMaxRetries,
		CacheSize:     DefaultCacheSize,
	}
}

// With overlays the non-zero values from other onto cfg
func (cfg Config) With(other Config) Config {
	if other.TrimEvents {
		cfg.TrimEvents = other.TrimEvents
	}
	if other.SnapshotRatio != 0 {
		cfg.SnapshotRatio = other.SnapshotRatio
	}
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
	switch {
	case cfg.SnapshotRatio < 0:
		return ErrInvalidSnapshotRatio
	case cfg.MaxRetries <= 0:
		return ErrInvalidMaxRetries
	case cfg.CacheSize <= 0:
		return ErrInvalidCacheSize
	}
	return nil
}

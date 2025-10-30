package timebox

import "time"

type (
	Config struct {
		Store                StoreConfig
		MaxRetries           int
		CacheSize            int
		EnableSnapshotWorker bool
	}

	StoreConfig struct {
		Addr         string
		Password     string
		Prefix       string
		DB           int
		WorkerCount  int
		MaxQueueSize int
		SaveTimeout  time.Duration
	}
)

const (
	DefaultRedisEndpoint       = "localhost:6379"
	DefaultRedisPrefix         = "timebox"
	DefaultRedisDB             = 0
	DefaultSnapshotWorkers     = 4
	DefaultSnapshotQueueSize   = 1024
	DefaultSnapshotSaveTimeout = 30 * time.Second
	DefaultMaxRetries          = 16
	DefaultExecutorCacheSize   = 128
)

func DefaultConfig() Config {
	return Config{
		Store:                DefaultStoreConfig(),
		MaxRetries:           DefaultMaxRetries,
		CacheSize:            DefaultExecutorCacheSize,
		EnableSnapshotWorker: true,
	}
}

func DefaultStoreConfig() StoreConfig {
	return StoreConfig{
		Addr:         DefaultRedisEndpoint,
		Password:     "",
		DB:           DefaultRedisDB,
		Prefix:       DefaultRedisPrefix,
		WorkerCount:  DefaultSnapshotWorkers,
		MaxQueueSize: DefaultSnapshotQueueSize,
		SaveTimeout:  DefaultSnapshotSaveTimeout,
	}
}

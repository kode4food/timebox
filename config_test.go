package timebox_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestDefaultConfig(t *testing.T) {
	cfg := timebox.DefaultConfig()

	assert.Equal(t, timebox.DefaultMaxRetries, cfg.MaxRetries)
	assert.Equal(t, timebox.DefaultCacheSize, cfg.CacheSize)
	assert.False(t, cfg.Snapshot.Workers)
	assert.Equal(t, timebox.DefaultSnapshotWorkers, cfg.Snapshot.WorkerCount)
	assert.Equal(t, timebox.DefaultSnapshotQueueSize, cfg.Snapshot.MaxQueueSize)
	assert.Equal(t,
		timebox.DefaultSnapshotSaveTimeout,
		cfg.Snapshot.SaveTimeout,
	)
	assert.Equal(t, timebox.DefaultTrimEvents, cfg.Snapshot.TrimEvents)
}

func TestConfigWith(t *testing.T) {
	cfg := timebox.Configure(timebox.DefaultConfig(), timebox.Config{
		MaxRetries: 9,
		CacheSize:  17,
		Snapshot: timebox.SnapshotConfig{
			Workers:      true,
			WorkerCount:  2,
			MaxQueueSize: 7,
			SaveTimeout:  5 * time.Second,
			TrimEvents:   true,
		},
		Archiving: true,
	})

	assert.Equal(t, 9, cfg.MaxRetries)
	assert.Equal(t, 17, cfg.CacheSize)
	assert.True(t, cfg.Snapshot.Workers)
	assert.Equal(t, 2, cfg.Snapshot.WorkerCount)
	assert.Equal(t, 7, cfg.Snapshot.MaxQueueSize)
	assert.Equal(t, 5*time.Second, cfg.Snapshot.SaveTimeout)
	assert.True(t, cfg.Snapshot.TrimEvents)
	assert.True(t, cfg.Archiving)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  timebox.Config
		err  error
	}{
		{
			name: "Missing MaxRetries",
			cfg:  timebox.Config{},
			err:  timebox.ErrInvalidMaxRetries,
		},
		{
			name: "Missing CacheSize",
			cfg:  timebox.Config{MaxRetries: 1},
			err:  timebox.ErrInvalidCacheSize,
		},
		{
			name: "Missing WorkerCount",
			cfg:  timebox.Config{MaxRetries: 1, CacheSize: 1},
			err:  timebox.ErrInvalidWorkerCount,
		},
		{
			name: "Missing MaxQueueSize",
			cfg: timebox.Config{
				MaxRetries: 1,
				CacheSize:  1,
				Snapshot:   timebox.SnapshotConfig{WorkerCount: 1},
			},
			err: timebox.ErrInvalidMaxQueueSize,
		},
		{
			name: "Missing SaveTimeout",
			cfg: timebox.Config{
				MaxRetries: 1,
				CacheSize:  1,
				Snapshot: timebox.SnapshotConfig{
					WorkerCount:  1,
					MaxQueueSize: 1,
				},
			},
			err: timebox.ErrInvalidSaveTimeout,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

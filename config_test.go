package timebox_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestDefaultConfig(t *testing.T) {
	cfg := timebox.DefaultConfig()

	assert.Equal(t, timebox.DefaultMaxRetries, cfg.MaxRetries)
	assert.Equal(t, timebox.DefaultCacheSize, cfg.CacheSize)
	assert.Equal(t, timebox.DefaultTrimEvents, cfg.TrimEvents)
	assert.Equal(t, timebox.DefaultSnapshotRatio, cfg.SnapshotRatio)
}

func TestConfigWith(t *testing.T) {
	cfg := timebox.Configure(timebox.DefaultConfig(), timebox.Config{
		MaxRetries:    9,
		CacheSize:     17,
		TrimEvents:    true,
		SnapshotRatio: 2.5,
	})

	assert.Equal(t, 9, cfg.MaxRetries)
	assert.Equal(t, 17, cfg.CacheSize)
	assert.True(t, cfg.TrimEvents)
	assert.Equal(t, 2.5, cfg.SnapshotRatio)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  timebox.Config
		err  error
	}{
		{
			name: "Negative SnapshotRatio",
			cfg: timebox.Config{
				SnapshotRatio: -1,
				MaxRetries:    1,
				CacheSize:     1,
			},
			err: timebox.ErrInvalidSnapshotRatio,
		},
		{
			name: "Missing MaxRetries",
			cfg:  timebox.Config{SnapshotRatio: 0},
			err:  timebox.ErrInvalidMaxRetries,
		},
		{
			name: "Missing CacheSize",
			cfg: timebox.Config{
				SnapshotRatio: 0,
				MaxRetries:    1,
			},
			err: timebox.ErrInvalidCacheSize,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

func TestConfigValidateValid(t *testing.T) {
	err := timebox.Config{
		SnapshotRatio: 0,
		MaxRetries:    1,
		CacheSize:     1,
	}.Validate()
	assert.NoError(t, err)
}

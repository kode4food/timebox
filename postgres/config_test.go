package postgres_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

func TestDefaultConfig(t *testing.T) {
	cfg := postgres.DefaultConfig()

	assert.Equal(t, postgres.DefaultURL, cfg.URL)
	assert.Equal(t, postgres.DefaultPrefix, cfg.Prefix)
	assert.EqualValues(t, postgres.DefaultMaxConns, cfg.MaxConns)
	assert.Equal(t, timebox.DefaultConfig(), cfg.Timebox)
}

func TestConfigWith(t *testing.T) {
	base := postgres.DefaultConfig()
	cfg := base.With(postgres.Config{
		URL:      "postgres://example.com:5432/app?sslmode=disable",
		Prefix:   "app",
		MaxConns: 32,
		Timebox: timebox.Config{
			CacheSize: 1024,
		},
	})

	assert.Equal(t, "postgres://example.com:5432/app?sslmode=disable", cfg.URL)
	assert.Equal(t, "app", cfg.Prefix)
	assert.EqualValues(t, 32, cfg.MaxConns)
	assert.Equal(t, 1024, cfg.Timebox.CacheSize)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  postgres.Config
		want error
	}{
		{
			name: "missing url",
			cfg: postgres.Config{
				Prefix:   "test",
				MaxConns: 96,
			},
			want: postgres.ErrURLRequired,
		},
		{
			name: "missing prefix",
			cfg: postgres.Config{
				URL:      "postgres://localhost:5432/app?sslmode=disable",
				MaxConns: 96,
			},
			want: postgres.ErrPrefixRequired,
		},
		{
			name: "invalid max conns",
			cfg: postgres.Config{
				URL:      "postgres://localhost:5432/app?sslmode=disable",
				Prefix:   "test",
				MaxConns: -1,
			},
			want: postgres.ErrInvalidMaxConns,
		},
		{
			name: "invalid timebox",
			cfg: postgres.Config{
				URL:      "postgres://localhost:5432/app?sslmode=disable",
				Prefix:   "test",
				MaxConns: 96,
				Timebox: timebox.Config{
					MaxRetries: 0,
				},
			},
			want: timebox.ErrInvalidMaxRetries,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			assert.ErrorIs(t, err, tt.want)
		})
	}
}

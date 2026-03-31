package redis_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox/redis"
)

func TestDefaultConfig(t *testing.T) {
	cfg := redis.DefaultConfig()

	assert.Equal(t, redis.DefaultEndpoint, cfg.Addr)
	assert.Equal(t, redis.DefaultPrefix, cfg.Prefix)
	assert.Equal(t, redis.DefaultDB, cfg.DB)
}

func TestConfigWith(t *testing.T) {
	cfg := redis.DefaultConfig().With(redis.Config{
		Addr:   "127.0.0.1:6380",
		Prefix: "orders",
		Shard:  "blue",
	})

	assert.Equal(t, "127.0.0.1:6380", cfg.Addr)
	assert.Equal(t, "orders", cfg.Prefix)
	assert.Equal(t, "blue", cfg.Shard)
}

func TestConfigWithFields(t *testing.T) {
	cfg := redis.DefaultConfig().With(redis.Config{
		Password: "secret",
		DB:       7,
	})

	assert.Equal(t, "secret", cfg.Password)
	assert.Equal(t, 7, cfg.DB)
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  redis.Config
		err  error
	}{
		{
			name: "Negative DB",
			cfg: redis.Config{
				Addr:   redis.DefaultEndpoint,
				Prefix: redis.DefaultPrefix,
				DB:     -1,
			},
			err: redis.ErrInvalidDB,
		},
		{
			name: "Invalid Shard",
			cfg: redis.Config{
				Addr:   redis.DefaultEndpoint,
				Prefix: redis.DefaultPrefix,
				Shard:  "blue:red",
			},
			err: redis.ErrInvalidShard,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

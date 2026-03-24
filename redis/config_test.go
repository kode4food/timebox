package redis_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/redis"
)

func TestDefaultConfig(t *testing.T) {
	cfg := redis.DefaultConfig()

	assert.Equal(t, redis.DefaultEndpoint, cfg.Addr)
	assert.Equal(t, redis.DefaultPrefix, cfg.Prefix)
	assert.Equal(t, redis.DefaultDB, cfg.DB)
	assert.NotNil(t, cfg.JoinKey)
	assert.NotNil(t, cfg.ParseKey)
	assert.Equal(t, timebox.DefaultMaxRetries, cfg.Timebox.MaxRetries)
	assert.Equal(t, timebox.DefaultCacheSize, cfg.Timebox.CacheSize)
}

func TestConfigWith(t *testing.T) {
	cfg := timebox.Configure(redis.DefaultConfig(), redis.Config{
		Timebox: timebox.Config{
			MaxRetries: 9,
			CacheSize:  17,
			Snapshot: timebox.SnapshotConfig{
				Workers:      true,
				WorkerCount:  2,
				MaxQueueSize: 7,
				TrimEvents:   true,
			},
		},
		Addr:   "127.0.0.1:6380",
		Prefix: "orders",
		Shard:  "blue",
	})

	assert.Equal(t, "127.0.0.1:6380", cfg.Addr)
	assert.Equal(t, "orders", cfg.Prefix)
	assert.Equal(t, "blue", cfg.Shard)
	assert.Equal(t, 9, cfg.Timebox.MaxRetries)
	assert.Equal(t, 17, cfg.Timebox.CacheSize)
	assert.True(t, cfg.Timebox.Snapshot.Workers)
	assert.Equal(t, 2, cfg.Timebox.Snapshot.WorkerCount)
	assert.Equal(t, 7, cfg.Timebox.Snapshot.MaxQueueSize)
	assert.True(t, cfg.Timebox.Snapshot.TrimEvents)
}

func TestConfigWithFields(t *testing.T) {
	join := func(id timebox.AggregateID) string {
		return "joined:" + id.Join(":")
	}
	parse := func(s string) timebox.AggregateID {
		return timebox.NewAggregateID(timebox.ID(s))
	}

	cfg := timebox.Configure(redis.DefaultConfig(), redis.Config{
		Password: "secret",
		DB:       7,
		JoinKey:  join,
		ParseKey: parse,
	})

	assert.Equal(t, "secret", cfg.Password)
	assert.Equal(t, 7, cfg.DB)
	assert.Equal(t, "joined:one", cfg.JoinKey(timebox.NewAggregateID("one")))
	assert.Equal(t, timebox.NewAggregateID("one"), cfg.ParseKey("one"))
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  redis.Config
		err  error
	}{
		{
			name: "Missing JoinKey",
			cfg: redis.Config{
				Timebox:  validTimeboxConfig(),
				Addr:     redis.DefaultEndpoint,
				Prefix:   redis.DefaultPrefix,
				ParseKey: redis.ParseKey,
			},
			err: redis.ErrJoinKeyRequired,
		},
		{
			name: "Missing ParseKey",
			cfg: redis.Config{
				Timebox: validTimeboxConfig(),
				Addr:    redis.DefaultEndpoint,
				Prefix:  redis.DefaultPrefix,
				JoinKey: redis.JoinKey,
			},
			err: redis.ErrParseKeyRequired,
		},
		{
			name: "Negative DB",
			cfg: redis.Config{
				Timebox:  validTimeboxConfig(),
				Addr:     redis.DefaultEndpoint,
				Prefix:   redis.DefaultPrefix,
				DB:       -1,
				JoinKey:  redis.JoinKey,
				ParseKey: redis.ParseKey,
			},
			err: redis.ErrInvalidDB,
		},
		{
			name: "Invalid Shard",
			cfg: redis.Config{
				Timebox:  validTimeboxConfig(),
				Addr:     redis.DefaultEndpoint,
				Prefix:   redis.DefaultPrefix,
				Shard:    "blue:red",
				JoinKey:  redis.JoinKey,
				ParseKey: redis.ParseKey,
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

func TestJoinParseKey(t *testing.T) {
	id := timebox.NewAggregateID("order-1", `path\part`, `%done`)
	joined := redis.JoinKey(id)
	parsed := redis.ParseKey(joined)

	assert.Equal(t, id, parsed)
}

func validTimeboxConfig() timebox.Config {
	return timebox.Config{
		MaxRetries: 1,
		CacheSize:  1,
		Snapshot: timebox.SnapshotConfig{
			WorkerCount:  1,
			MaxQueueSize: 1,
		},
	}
}

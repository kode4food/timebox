package redis_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	tbredis "github.com/kode4food/timebox/redis"
)

func TestStoreDefaultKeys(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		testStoreConfig(server.Addr(), func(cfg *tbredis.Config) {
			cfg.Prefix = "noslot"
		}),
		timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("order", "1")
	ev := &timebox.Event{
		Type:      "incremented",
		Timestamp: time.Now(),
		Data:      json.RawMessage(`5`),
	}
	assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
	assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 1}, 1))

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	keys, err := client.Keys(context.Background(), "noslot:*").Result()
	assert.NoError(t, err)
	assert.Contains(t, keys, "noslot:order:1:events")
	assert.Contains(t, keys, "noslot:order:1:snapshot:val")
	assert.Contains(t, keys, "noslot:order:1:snapshot:seq")
	assert.NotContains(t, keys, "noslot:order:1:snapshot:base")
	assert.NotContains(t, keys, "noslot:{order:1}:events")
}

func TestStoreShardKeys(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		testStoreConfig(server.Addr(), func(cfg *tbredis.Config) {
			cfg.Shard = "blue"
		}),
		timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("order", "1")
	ev := &timebox.Event{
		Type:      "incremented",
		Timestamp: time.Now(),
		Data:      json.RawMessage(`5`),
	}
	assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
	assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 1}, 1))

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	keys, err := client.Keys(context.Background(), "timebox:{blue}:*").Result()
	assert.NoError(t, err)
	assert.Contains(t, keys, "timebox:{blue}:order:1:events")
	assert.Contains(t, keys, "timebox:{blue}:order:1:snapshot:val")
	assert.Contains(t, keys, "timebox:{blue}:order:1:snapshot:seq")
	assert.NotContains(t, keys, "timebox:{blue}:order:1:snapshot:base")
	assert.NotContains(t, keys, "{blue}:order:1:events")

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
}

func TestNewStorePingError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	addr := server.Addr()
	server.Close()

	store, err := newStore(tbredis.Config{Addr: addr}, timebox.Config{})
	assert.Error(t, err)
	assert.Nil(t, store)
}

package redis_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	tbredis "github.com/kode4food/timebox/redis"
)

func TestCorruptEvents(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	cfg := testConfig(server.Addr(), func(cfg *tbredis.Config) {
		cfg.Prefix = "corrupt"
	})

	store, err := newStore(t, cfg)
	assert.NoError(t, err)

	id := timebox.NewAggregateID("order", "1")
	eventsKey := cfg.Prefix + ":" + joinAggregateID(id) + ":events"

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	err = client.RPush(context.Background(), eventsKey, "not-json").Err()
	assert.NoError(t, err)

	events, err := store.GetEvents(id, 0)
	assert.Error(t, err)
	assert.Nil(t, events)
}

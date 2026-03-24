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

func TestSnapshotCorrupt(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	storeCfg := testStoreConfig(server.Addr(), func(cfg *tbredis.Config) {
		cfg.Prefix = "corrupt-snapshot"
	})

	store, err := newStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("order", "1")

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	err = client.Set(
		context.Background(),
		storeCfg.Prefix+":"+id.Join(":")+":snapshot:val",
		"not-json",
		0,
	).Err()
	assert.NoError(t, err)

	var target map[string]any
	res, err := store.GetSnapshot(id, &target)
	assert.Error(t, err)
	assert.Nil(t, res)
}

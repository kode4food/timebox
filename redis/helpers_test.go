package redis_test

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/id"
	tbredis "github.com/kode4food/timebox/redis"
)

var joinAggregateID, _ = id.MakeCodec(':')

func newStore(cfg tbredis.Config, storeCfg timebox.Config) (*timebox.Store, error) {
	p, err := tbredis.NewPersistence(cfg)
	if err != nil {
		return nil, err
	}
	return p.NewStore(storeCfg)
}

func newPersistence(cfgs ...tbredis.Config) (*tbredis.Persistence, error) {
	return tbredis.NewPersistence(cfgs...)
}

func testConfig(addr string, mutate func(*tbredis.Config)) tbredis.Config {
	cfg := tbredis.Config{Addr: addr}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func testStoreConfig(addr string, mutate func(*tbredis.Config)) tbredis.Config {
	return testConfig(addr, mutate)
}

func withPersistence(
	t *testing.T, mutate func(*tbredis.Config),
	fn func(context.Context, *tbredis.Persistence, *redis.Client),
) {
	t.Helper()

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	p, err := newPersistence(testConfig(server.Addr(), mutate))
	assert.NoError(t, err)
	defer func() { _ = p.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	fn(context.Background(), p, client)
}

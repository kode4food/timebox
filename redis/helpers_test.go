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

// joinAggregateID skips escaping, which no test ID needs
func joinAggregateID(id timebox.AggregateID) string {
	return string(id.Type) + ":" + string(id.Key)
}

func testConfig(addr string, mutate func(*tbredis.Config)) tbredis.Config {
	cfg := tbredis.Config{Addr: addr}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func newStore(
	t *testing.T, cfg tbredis.Config, tbCfgs ...timebox.Config,
) (*timebox.Store, error) {
	t.Helper()

	b, err := tbredis.Open(cfg)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = b.Close() })
	return b.NewStore(tbCfgs...)
}

func withBackend(
	t *testing.T, mutate func(*tbredis.Config),
	fn func(context.Context, *tbredis.Backend, *redis.Client),
) {
	t.Helper()

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	b, err := tbredis.Open(testConfig(server.Addr(), mutate))
	assert.NoError(t, err)
	defer func() { _ = b.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	fn(context.Background(), b, client)
}

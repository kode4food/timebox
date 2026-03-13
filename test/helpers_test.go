package integration_test

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

const eventIncremented timebox.EventType = "incremented"

func newStore(cfgs ...tbredis.Config) (*timebox.Store, error) {
	return tbredis.NewStore(cfgs...)
}

func testStoreConfig(
	addr string, mutate func(*tbredis.Config),
) tbredis.Config {
	cfg := tbredis.Config{Addr: addr}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func withIndexedStore(
	t *testing.T, trimEvents bool, prefix string, indexer timebox.Indexer,
	fn func(context.Context, *timebox.Store, *redis.Client),
) {
	t.Helper()

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		testStoreConfig(server.Addr(), func(cfg *tbredis.Config) {
			cfg.Prefix = prefix
			cfg.Timebox.Snapshot.TrimEvents = trimEvents
			cfg.Timebox.Indexer = indexer
		}),
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	fn(context.Background(), store, client)
}

func withStatusStore(
	t *testing.T, trimEvents bool, prefix string,
	fn func(context.Context, *timebox.Store, *redis.Client),
) {
	withIndexedStore(t, trimEvents, prefix, statusIndexer, fn)
}

func combinedEvent(
	id timebox.AggregateID, status string, env string,
) *timebox.Event {
	data, err := json.Marshal(map[string]string{
		"status": status,
		"env":    env,
	})
	if err != nil {
		panic(err)
	}

	return &timebox.Event{
		Timestamp:   time.Now(),
		Type:        eventIncremented,
		AggregateID: id,
		Data:        data,
	}
}

func combinedIndexer(events []*timebox.Event) []*timebox.Index {
	var res []*timebox.Index
	for _, ev := range events {
		data := map[string]string{}
		if err := json.Unmarshal(ev.Data, &data); err != nil {
			continue
		}

		status, ok := data["status"]
		if !ok {
			continue
		}

		res = append(res, &timebox.Index{
			Status: &status,
			Labels: map[string]string{"env": data["env"]},
		})
	}
	return res
}

func eventListKey(prefix string, id timebox.AggregateID) string {
	return prefix + ":" + id.Join(":") + ":events"
}

package timebox_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestVersionConflictError(t *testing.T) {
	err := &timebox.VersionConflictError{
		ExpectedSequence: 0,
		ActualSequence:   5,
		NewEvents:        []*timebox.Event{{}, {}},
	}

	assert.Contains(t, err.Error(), "version conflict")
	assert.Contains(t, err.Error(), "expected sequence 0")
	assert.Contains(t, err.Error(), "but at 5")
}

func TestStore(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("test", "1")

	ev := &timebox.Event{
		Type:        EventIncremented,
		AggregateID: id,
		Timestamp:   time.Now(),
		Data:        json.RawMessage(`5`),
	}

	err = store.AppendEvents(ctx, id, 0, []*timebox.Event{ev})
	assert.NoError(t, err)

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, EventIncremented, events[0].Type)
}

func TestStoreDefaultKeys(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "noslot"
		}),
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")
	ev := &timebox.Event{
		Type:      EventIncremented,
		Timestamp: time.Now(),
		Data:      json.RawMessage(`5`),
	}
	assert.NoError(t, store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}))
	assert.NoError(t, store.PutSnapshot(ctx, id, map[string]int{"value": 1}, 1))

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	keys, err := client.Keys(ctx, "noslot:*").Result()
	assert.NoError(t, err)
	assert.Contains(t, keys, "noslot:order:1:events")
	assert.Contains(t, keys, "noslot:order:1:snapshot:val")
	assert.Contains(t, keys, "noslot:order:1:snapshot:seq")
	assert.NotContains(t, keys, "noslot:{order:1}:events")
}

func TestStoreShardKeys(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Shard = "blue"
		}),
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")
	ev := &timebox.Event{
		Type:      EventIncremented,
		Timestamp: time.Now(),
		Data:      json.RawMessage(`5`),
	}
	assert.NoError(t, store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}))
	assert.NoError(t, store.PutSnapshot(ctx, id, map[string]int{"value": 1}, 1))

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	keys, err := client.Keys(ctx, "timebox:{blue}:*").Result()
	assert.NoError(t, err)
	assert.Contains(t, keys, "timebox:{blue}:order:1:events")
	assert.Contains(t, keys, "timebox:{blue}:order:1:snapshot:val")
	assert.Contains(t, keys, "timebox:{blue}:order:1:snapshot:seq")
	assert.NotContains(t, keys, "{blue}:order:1:events")

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
}

func TestNewStoreInvalidConfig(t *testing.T) {
	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	tests := []struct {
		name string
		cfg  timebox.Config
		err  error
	}{
		{
			name: "Negative MaxRetries",
			cfg: timebox.Config{
				MaxRetries: -1,
			},
			err: timebox.ErrInvalidMaxRetries,
		},
		{
			name: "Negative CacheSize",
			cfg: timebox.Config{
				CacheSize: -1,
			},
			err: timebox.ErrInvalidCacheSize,
		},
		{
			name: "Negative WorkerCount",
			cfg: timebox.Config{
				Snapshot: timebox.SnapshotConfig{
					WorkerCount: -1,
				},
			},
			err: timebox.ErrInvalidWorkerCount,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := tb.NewStore(tc.cfg)
			assert.ErrorIs(t, err, tc.err)
			assert.Nil(t, store)
		})
	}
}

func TestConfigValidateRequiresKeyFuncs(t *testing.T) {
	storeCfg := timebox.Config{
		Redis: redisConfig(),
	}
	err := storeCfg.Validate()
	assert.ErrorIs(t, err, timebox.ErrInvalidMaxRetries)

	storeCfg = timebox.Config{
		Redis:      redisConfig(),
		MaxRetries: 1,
	}
	err = storeCfg.Validate()
	assert.ErrorIs(t, err, timebox.ErrInvalidCacheSize)

	storeCfg = timebox.Config{
		Redis:      redisConfig(),
		MaxRetries: 1,
		CacheSize:  1,
	}
	err = storeCfg.Validate()
	assert.ErrorIs(t, err, timebox.ErrInvalidWorkerCount)

	storeCfg = timebox.Config{
		Redis:      redisConfig(),
		MaxRetries: 1,
		CacheSize:  1,
		Snapshot: timebox.SnapshotConfig{
			WorkerCount:  1,
			MaxQueueSize: 0,
		},
	}
	err = storeCfg.Validate()
	assert.ErrorIs(t, err, timebox.ErrInvalidMaxQueueSize)

	storeCfg = timebox.Config{
		Redis:      redisConfig(),
		MaxRetries: 1,
		CacheSize:  1,
		Snapshot: timebox.SnapshotConfig{
			WorkerCount:  1,
			MaxQueueSize: 1,
			SaveTimeout:  0,
		},
	}
	err = storeCfg.Validate()
	assert.ErrorIs(t, err, timebox.ErrInvalidSaveTimeout)

	storeCfg = timebox.Config{
		Redis: timebox.RedisConfig{
			Addr:     timebox.DefaultRedisEndpoint,
			Prefix:   timebox.DefaultRedisPrefix,
			ParseKey: timebox.ParseKey,
		},
		MaxRetries: 1,
		CacheSize:  1,
		Snapshot: timebox.SnapshotConfig{
			WorkerCount:  1,
			MaxQueueSize: 1,
			SaveTimeout:  time.Second,
		},
	}
	err = storeCfg.Validate()
	assert.ErrorIs(t, err, timebox.ErrJoinKeyRequired)

	storeCfg = timebox.Config{
		Redis: timebox.RedisConfig{
			Addr:    timebox.DefaultRedisEndpoint,
			Prefix:  timebox.DefaultRedisPrefix,
			JoinKey: timebox.JoinKey,
		},
		MaxRetries: 1,
		CacheSize:  1,
		Snapshot: timebox.SnapshotConfig{
			WorkerCount:  1,
			MaxQueueSize: 1,
			SaveTimeout:  time.Second,
		},
	}
	err = storeCfg.Validate()
	assert.ErrorIs(t, err, timebox.ErrParseKeyRequired)
}

func TestAppendConflict(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "conflict"
		}),
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	err = store.AppendEvents(ctx, id, 1, []*timebox.Event{ev})
	assert.Error(t, err)
}

func TestListAggregates(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "list"
		}),
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id1 := timebox.NewAggregateID("order", "1")
	id2 := timebox.NewAggregateID("order", "2")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t, store.AppendEvents(ctx, id1, 0, []*timebox.Event{ev}))
	assert.NoError(t, store.AppendEvents(ctx, id2, 0, []*timebox.Event{ev}))

	ids, err := store.ListAggregates(ctx, id1)
	assert.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Equal(t, id1, ids[0])
}

func TestCorruptEvents(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	storeCfg := testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
		cfg.Redis.Prefix = "corrupt"
	})

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")
	eventsKey := storeCfg.Redis.Prefix + ":" + id.Join(":") + ":events"

	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer func() { _ = client.Close() }()

	err = client.RPush(ctx, eventsKey, "not-json").Err()
	assert.NoError(t, err)

	events, err := store.GetEvents(ctx, id, 0)
	assert.Error(t, err)
	assert.Nil(t, events)
}

func TestGetEventsEmpty(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "empty-events"
		}),
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	events, err := store.GetEvents(
		context.Background(), timebox.NewAggregateID("counter", "1"), 0,
	)
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestStoreTrimmedPlainAppend(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "trimmed-plain"
			cfg.Snapshot.TrimEvents = true
		}),
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")
	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`5`),
	}

	assert.NoError(t, store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}))

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, EventIncremented, events[0].Type)
}

func TestStoreCombinedIndexing(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			withIndexedStore(t,
				trimEvents, "combined-index", combinedIndexer,
				func(
					ctx context.Context, store *timebox.Store,
					_ *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")

					assert.NoError(t,
						store.AppendEvents(ctx, id, 0, []*timebox.Event{
							combinedEvent(id, "active", "prod"),
						}),
					)

					statuses, err := store.ListAggregatesByStatus(
						ctx, "active",
					)
					assert.NoError(t, err)
					assert.Len(t, statuses, 1)
					assert.Equal(t, id, statuses[0].ID)

					values, err := store.ListLabelValues(ctx, "env")
					assert.NoError(t, err)
					assert.Equal(t, []string{"prod"}, values)

					ids, err := store.ListAggregatesByLabel(
						ctx, "env", "prod",
					)
					assert.NoError(t, err)
					assert.Equal(t, []timebox.AggregateID{id}, ids)
				},
			)
		})
	}
}

func TestNewStorePingError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	addr := server.Addr()
	server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(addr, nil))
	assert.Error(t, err)
	assert.Nil(t, store)
}

func withIndexedStore(
	t *testing.T, trimEvents bool, prefix string, indexer timebox.Indexer,
	fn func(context.Context, *timebox.Store, *redis.Client),
) {
	t.Helper()

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Prefix = prefix
			cfg.Snapshot.TrimEvents = trimEvents
			cfg.Indexer = indexer
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
		Type:        EventIncremented,
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

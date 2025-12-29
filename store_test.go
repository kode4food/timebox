package timebox_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
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

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
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

func TestAppendConflict(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "conflict"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
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

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "list"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
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

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "corrupt"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")
	eventsKey := storeCfg.Prefix + ":" + id.Join(":") + ":events"

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

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "empty-events"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	events, err := store.GetEvents(
		context.Background(), timebox.NewAggregateID("counter", "1"), 0,
	)
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestGetSnapshotEmpty(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "empty-snapshot"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	var state CounterState
	snap, err := store.GetSnapshot(
		context.Background(), timebox.NewAggregateID("counter", "1"), &state,
	)
	assert.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Len(t, snap.AdditionalEvents, 0)
	assert.Equal(t, int64(0), snap.NextSequence)
}

func TestGetSnapshotCorruptPayload(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "corrupt-snapshot"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")
	snapValKey := storeCfg.Prefix + ":" + id.Join(":") + ":snapshot:val"
	snapSeqKey := storeCfg.Prefix + ":" + id.Join(":") + ":snapshot:seq"

	client := redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
	defer func() { _ = client.Close() }()

	assert.NoError(t, client.Set(ctx, snapValKey, "not-json", 0).Err())
	assert.NoError(t, client.Set(ctx, snapSeqKey, 0, 0).Err())

	var state CounterState
	snap, err := store.GetSnapshot(ctx, id, &state)
	assert.Error(t, err)
	assert.Nil(t, snap)
}

func TestNewStorePingError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	addr := server.Addr()
	server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = addr

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.Error(t, err)
	assert.Nil(t, store)
}

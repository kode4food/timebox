package timebox_test

import (
	"context"
	"encoding/json"
	"strconv"
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

func TestStoreDefaultKeys(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "noslot"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
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

func TestStoreJoinKeySlotted(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "slotted"
	storeCfg.JoinKey = timebox.JoinKeySlotted(1)
	storeCfg.ParseKey = timebox.ParseKeySlotted(1)

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id1 := timebox.NewAggregateID("flow", "abc")
	id2 := timebox.NewAggregateID("flow", "xyz")
	ev := &timebox.Event{
		Type:      EventIncremented,
		Timestamp: time.Now(),
		Data:      json.RawMessage(`5`),
	}
	assert.NoError(t, store.AppendEvents(ctx, id1, 0, []*timebox.Event{ev}))
	assert.NoError(t, store.AppendEvents(ctx, id2, 0, []*timebox.Event{ev}))
	assert.NoError(t,
		store.PutSnapshot(ctx, id1, map[string]int{"value": 1}, 1),
	)

	// verify key format: slot key in braces, remaining component outside
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	keys, err := client.Keys(ctx, "slotted:*").Result()
	assert.NoError(t, err)
	assert.Contains(t, keys, "slotted:{flow}:abc:events")
	assert.Contains(t, keys, "slotted:{flow}:xyz:events")
	assert.Contains(t, keys, "slotted:{flow}:abc:snapshot:val")
	assert.Contains(t, keys, "slotted:{flow}:abc:snapshot:seq")

	// verify round-trip: GetEvents returns correct events with correct ID
	events, err := store.GetEvents(ctx, id1, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, id1, events[0].AggregateID)

	// verify ListAggregates reconstructs the full ID correctly
	ids, err := store.ListAggregates(ctx, timebox.NewAggregateID("flow", "abc"))
	assert.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Equal(t, id1, ids[0])
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

func TestStoreStatusIndexing(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("NoIndexNoMutation", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-no-index", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")

					err := store.AppendEvents(
						ctx, id, 0, []*timebox.Event{statusEvent(id, nil)},
					)
					assert.NoError(t, err)

					raw, err := client.LIndex(
						ctx, eventListKey("status-no-index", id), 0,
					).Result()
					assert.NoError(t, err)
					assert.NotContains(t, raw, `"index"`)

					assertNoCurrentStatus(t, ctx, client, "status-no-index", id)
					assertStatusMembership(
						t, ctx, client, "status-no-index", "active", id, false,
					)

					events, err := store.GetEvents(ctx, id, 0)
					assert.NoError(t, err)
					assert.Len(t, events, 1)
				})
			})

			t.Run("SingleStatusSetsMembership", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-single", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &active),
					})
					assert.NoError(t, err)

					raw, err := client.LIndex(
						ctx, eventListKey("status-single", id), 0,
					).Result()
					assert.NoError(t, err)
					assert.NotContains(t, raw, `"index"`)

					assertCurrentStatus(
						t, ctx, client, "status-single", id, active,
					)
					assertStatusMembership(
						t, ctx, client, "status-single", active, id, true,
					)

					events, err := store.GetEvents(ctx, id, 0)
					assert.NoError(t, err)
					assert.Len(t, events, 1)
				})
			})

			t.Run("FinalStatusWinsWithinBatch", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-batch", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					pending := "pending"
					processing := "processing"
					active := "active"

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &pending),
						statusEvent(id, &processing),
						statusEvent(id, &active),
					})
					assert.NoError(t, err)

					assertCurrentStatus(
						t, ctx, client, "status-batch", id, active,
					)
					assertStatusMembership(
						t, ctx, client, "status-batch", pending, id, false,
					)
					assertStatusMembership(
						t, ctx, client, "status-batch", processing, id, false,
					)
					assertStatusMembership(
						t, ctx, client, "status-batch", active, id, true,
					)
				})
			})

			t.Run("TransitionMovesBetweenSets", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-transition", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					paused := "paused"

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &active),
					})
					assert.NoError(t, err)

					err = store.AppendEvents(ctx, id, 1, []*timebox.Event{
						statusEvent(id, &paused),
					})
					assert.NoError(t, err)

					assertCurrentStatus(
						t, ctx, client, "status-transition", id, paused,
					)
					assertStatusMembership(
						t, ctx, client, "status-transition", active, id, false,
					)
					assertStatusMembership(
						t, ctx, client, "status-transition", paused, id, true,
					)
				})
			})

			t.Run("ClearStatusRemovesMembership", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-clear", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					cleared := ""

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &active),
					})
					assert.NoError(t, err)

					err = store.AppendEvents(ctx, id, 1, []*timebox.Event{
						statusEvent(id, &cleared),
					})
					assert.NoError(t, err)

					assertNoCurrentStatus(t, ctx, client, "status-clear", id)
					assertStatusMembership(
						t, ctx, client, "status-clear", active, id, false,
					)
				})
			})

			t.Run("ConflictDoesNotMutateStatus", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-conflict", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					paused := "paused"

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &active),
					})
					assert.NoError(t, err)

					err = store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &paused),
					})
					assert.Error(t, err)

					assertCurrentStatus(
						t, ctx, client, "status-conflict", id, active,
					)
					assertStatusMembership(
						t, ctx, client, "status-conflict", active, id, true,
					)
					assertStatusMembership(
						t, ctx, client, "status-conflict", paused, id, false,
					)
				})
			})
		})
	}
}

func withStatusStore(
	t *testing.T, trimEvents bool, prefix string,
	fn func(context.Context, *timebox.Store, *redis.Client),
) {
	t.Helper()

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = prefix
	storeCfg.TrimEvents = trimEvents
	storeCfg.Indexer = statusIndexer

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	fn(context.Background(), store, client)
}

func statusEvent(id timebox.AggregateID, status *string) *timebox.Event {
	data := json.RawMessage(`1`)
	if status != nil {
		data = json.RawMessage(strconv.Quote(*status))
	}

	return &timebox.Event{
		Timestamp:   time.Now(),
		Type:        EventIncremented,
		AggregateID: id,
		Data:        data,
	}
}

func statusIndexer(events []*timebox.Event) []*timebox.Index {
	res := []*timebox.Index{}
	for _, ev := range events {
		var status string
		if err := json.Unmarshal(ev.Data, &status); err == nil {
			res = append(res, &timebox.Index{Status: &status})
		}
	}
	return res
}

func eventListKey(prefix string, id timebox.AggregateID) string {
	return prefix + ":" + id.Join(":") + ":events"
}

func currentStatusKey(prefix string) string {
	return prefix + ":status"
}

func statusSetKey(prefix, status string) string {
	return prefix + ":status:" + status
}

func assertCurrentStatus(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	id timebox.AggregateID, expected string,
) {
	t.Helper()

	actual, err := client.HGet(
		ctx, currentStatusKey(prefix), id.Join(":"),
	).Result()
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func assertNoCurrentStatus(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	id timebox.AggregateID,
) {
	t.Helper()

	_, err := client.HGet(ctx, currentStatusKey(prefix), id.Join(":")).Result()
	assert.ErrorIs(t, err, redis.Nil)
}

func assertStatusMembership(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	status string, id timebox.AggregateID, expected bool,
) {
	t.Helper()

	members, err := client.SMembers(ctx, statusSetKey(prefix, status)).Result()
	assert.NoError(t, err)
	if expected {
		assert.Contains(t, members, id.Join(":"))
		return
	}
	assert.NotContains(t, members, id.Join(":"))
}

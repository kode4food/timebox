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

func TestSnapshotWorkerSaveTimesOut(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "snapshot-timeout"
	storeCfg.WorkerCount = 1
	storeCfg.MaxQueueSize = 1
	storeCfg.SaveTimeout = 0

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "timeout")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`1`),
	}
	assert.NoError(t, store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}))

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	_, err = executor.Exec(ctx, id,
		func(*CounterState, *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)
	assert.NoError(t, err)

	time.Sleep(20 * time.Millisecond)
	var snap CounterState
	result, err := store.GetSnapshot(ctx, id, &snap)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int64(0), result.NextSequence)
}

func TestWithoutSnapshotWorker(t *testing.T) {
	server, tb, store, executor := setupTestExecutorWithoutSnapshotWorker(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "no-snapshot")

	state, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 10)
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	err = executor.SaveSnapshot(ctx, id)
	assert.NoError(t, err)
}

func TestSequenceWithSnapshot(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "snap-seq-test")

	_, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := timebox.Raise(ag, EventIncremented, 5); err != nil {
				return err
			}
			return timebox.Raise(ag, EventIncremented, 5)
		},
	)
	assert.NoError(t, err)

	err = executor.SaveSnapshot(ctx, id)
	assert.NoError(t, err)

	_, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := timebox.Raise(ag, EventIncremented, 3); err != nil {
				return err
			}
			return timebox.Raise(ag, EventIncremented, 3)
		},
	)
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(ctx, id, &state)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), snap.NextSequence)

	assert.Len(t, snap.AdditionalEvents, 2)
	assert.Equal(t, int64(2), snap.AdditionalEvents[0].Sequence)
	assert.Equal(t, int64(3), snap.AdditionalEvents[1].Sequence)
}

func TestSnapshotTrimsEvents(t *testing.T) {
	server, tb, store, executor := setupTestExecutorWithStoreConfig(t, func(cfg *timebox.StoreConfig) {
		cfg.TrimEvents = true
	})
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "trim-events")

	_, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			for range 3 {
				if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	assert.NoError(t, err)

	err = executor.SaveSnapshot(ctx, id)
	assert.NoError(t, err)

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 0)

	aggregates, err := store.ListAggregates(ctx, id)
	assert.NoError(t, err)
	assert.Len(t, aggregates, 1)
	assert.Equal(t, id, aggregates[0])

	_, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			for range 2 {
				if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	assert.NoError(t, err)

	events, err = store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, int64(3), events[0].Sequence)
	assert.Equal(t, int64(4), events[1].Sequence)
}

func TestLargeEventBatchWithSnapshot(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "large-batch")

	numEvents := 300

	state, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			for range numEvents {
				if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, numEvents, state.Value)

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, numEvents)

	for i := range numEvents {
		assert.Equal(t, int64(i), events[i].Sequence)
	}

	events, err = store.GetEvents(ctx, id, 150)
	assert.NoError(t, err)
	assert.Len(t, events, numEvents-150)
	assert.Equal(t, int64(150), events[0].Sequence)

	err = executor.SaveSnapshot(ctx, id)
	assert.NoError(t, err)

	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			for range 50 {
				if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, numEvents+50, state.Value)

	var snapState CounterState
	snap, err := store.GetSnapshot(ctx, id, &snapState)
	assert.NoError(t, err)
	assert.Equal(t, numEvents, snapState.Value)
	assert.Len(t, snap.AdditionalEvents, 50)
	for i := range 50 {
		assert.Equal(t, int64(numEvents+i), snap.AdditionalEvents[i].Sequence)
	}
}

func TestSnapshotWorker(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "snapshot-worker"
	storeCfg.WorkerCount = 1
	storeCfg.MaxQueueSize = 1
	storeCfg.SaveTimeout = time.Second

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "snapshot")
	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`1`),
	}
	err = store.AppendEvents(
		context.Background(), id, 0, []*timebox.Event{ev},
	)
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	_, err = executor.Exec(context.Background(), id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, 1, s.Value)
			return nil
		},
	)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		var snap CounterState
		result, err := store.GetSnapshot(context.Background(), id, &snap)
		if err != nil || result == nil {
			return false
		}
		return result.NextSequence > 0
	}, time.Second, 10*time.Millisecond)
}

func TestSaveSnapshotError(t *testing.T) {
	server, tb, store, _ := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	type BadState struct {
		Value chan int
	}

	executor := timebox.NewExecutor(
		store,
		func() *BadState { return &BadState{Value: make(chan int)} },
		timebox.Appliers[*BadState]{},
	)

	err := executor.SaveSnapshot(
		context.Background(),
		timebox.NewAggregateID("bad", "snapshot"),
	)
	assert.Error(t, err)
}

func TestSaveSnapshot(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("save", "snapshot")

	_, err := executor.Exec(ctx, id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		assert.Equal(t, 0, s.Value)
		return timebox.Raise(ag, EventIncremented, 2)
	})
	assert.NoError(t, err)

	err = executor.SaveSnapshot(ctx, id)
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(ctx, id, &state)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), snap.NextSequence)
	assert.Equal(t, 2, state.Value)
}

func TestSaveSnapshotLoadError(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("save", "snapshot-error")

	server.Close()

	err := executor.SaveSnapshot(ctx, id)
	assert.Error(t, err)
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
	snapValKey := storeCfg.Prefix + ":{" + id.Join(":") + "}:snapshot:val"
	snapSeqKey := storeCfg.Prefix + ":{" + id.Join(":") + "}:snapshot:seq"

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

func setupTestExecutorWithoutSnapshotWorker(t *testing.T) (
	*miniredis.Miniredis, *timebox.Timebox, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	server, err := miniredis.Run()
	assert.NoError(t, err)

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "test"
	cfg.Workers = false

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, tb, store, executor
}

package timebox_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

// Simple counter state for testing
type CounterState struct {
	Value int `json:"value"`
}

const (
	EventIncremented timebox.EventType = "incremented"
	EventDecremented timebox.EventType = "decremented"
	EventReset       timebox.EventType = "reset"
)

var appliers = timebox.Appliers[*CounterState]{
	EventIncremented: func(
		state *CounterState, ev *timebox.Event,
	) *CounterState {
		var delta int
		_ = json.Unmarshal(ev.Data, &delta)
		res := *state
		res.Value = state.Value + delta
		return &res
	},
	EventDecremented: func(
		state *CounterState, ev *timebox.Event,
	) *CounterState {
		var delta int
		_ = json.Unmarshal(ev.Data, &delta)
		res := *state
		res.Value = state.Value - delta
		return &res
	},
	EventReset: func(state *CounterState, ev *timebox.Event) *CounterState {
		res := *state
		res.Value = 0
		return &res
	},
}

// Integration tests

func TestContext(t *testing.T) {
	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	ctx := tb.Context()
	assert.NotNil(t, ctx)
}

func TestNewTimeboxInvalidConfig(t *testing.T) {
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
		{
			name: "Negative MaxQueueSize",
			cfg: timebox.Config{
				Snapshot: timebox.SnapshotConfig{
					MaxQueueSize: -1,
				},
			},
			err: timebox.ErrInvalidMaxQueueSize,
		},
		{
			name: "Negative SaveTimeout",
			cfg: timebox.Config{
				Snapshot: timebox.SnapshotConfig{
					SaveTimeout: -time.Second,
				},
			},
			err: timebox.ErrInvalidSaveTimeout,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tb, err := timebox.NewTimebox(tc.cfg)
			assert.ErrorIs(t, err, tc.err)
			assert.Nil(t, tb)
		})
	}
}

func TestSparseConfig(t *testing.T) {
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

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	state, err := executor.Exec(
		context.Background(),
		timebox.NewAggregateID("counter", "1"),
		func(*CounterState, *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 0, state.Value)
}

func TestGetEventValue(t *testing.T) {
	type CachedData struct {
		Name string `json:"name"`
	}

	event := &timebox.Event{
		Type: "event.cached",
		Data: []byte(`{"name":"cached"}`),
	}

	data, err := timebox.GetEventValue[CachedData](event)
	assert.NoError(t, err)
	assert.Equal(t, "cached", data.Name)

	values, err := timebox.GetEventValue[map[string]any](event)
	assert.NoError(t, err)
	assert.Equal(t, "cached", values["name"])

	event.Data = []byte("not json")
	data, err = timebox.GetEventValue[CachedData](event)
	assert.NoError(t, err)
	assert.Equal(t, "cached", data.Name)

	_, err = timebox.GetEventValue[map[string]any](event)
	assert.Error(t, err)
}

func TestEventHubNotification(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	consumer := tb.GetHub().NewConsumer()
	defer consumer.Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "notif")

	go func() {
		_, _ = executor.Exec(ctx, id,
			func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
				return timebox.Raise(ag, EventIncremented, 1)
			},
		)
	}()

	select {
	case ev := <-consumer.Receive():
		assert.Equal(t, EventIncremented, ev.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestEventHubSequence(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	consumer := tb.GetHub().NewConsumer()
	defer consumer.Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "hub-seq-test")

	// Raise multiple events and verify sequences in event hub
	done := make(chan []*timebox.Event)
	go func() {
		var received []*timebox.Event
		for range 3 {
			select {
			case ev := <-consumer.Receive():
				received = append(received, ev)
			case <-time.After(1 * time.Second):
				return
			}
		}
		done <- received
	}()

	_, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)

	received := <-done
	assert.Len(t, received, 3)
	assert.Equal(t, int64(0), received[0].Sequence)
	assert.Equal(t, int64(1), received[1].Sequence)
	assert.Equal(t, int64(2), received[2].Sequence)
}

func TestEventHubAggregatePrefix(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	consumer := tb.GetHub().NewAggregateConsumer(
		timebox.NewAggregateID("flow"),
	)
	defer consumer.Close()

	ctx := context.Background()
	ids := []timebox.AggregateID{
		timebox.NewAggregateID("flow", "a"),
		timebox.NewAggregateID("flow", "b"),
		timebox.NewAggregateID("engine", "x"),
	}

	for _, id := range ids {
		_, err := executor.Exec(ctx, id,
			func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
				return timebox.Raise(ag, EventIncremented, 1)
			},
		)
		assert.NoError(t, err)
	}

	for range 2 {
		select {
		case ev := <-consumer.Receive():
			assert.Equal(t, timebox.ID("flow"), ev.AggregateID[0])
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for flow event")
		}
	}

	select {
	case ev := <-consumer.Receive():
		t.Fatalf("unexpected event for %v", ev.AggregateID)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestEventHubTypeFilterWithPrefix(t *testing.T) {
	const (
		eventWait = 1 * time.Second
		idleWait  = 100 * time.Millisecond
	)

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	consumer := tb.GetHub().NewAggregateConsumer(
		timebox.NewAggregateID("flow"),
		EventIncremented,
	)
	defer consumer.Close()

	ctx := context.Background()

	_, err = executor.Exec(ctx, timebox.NewAggregateID("flow", "a"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			return timebox.Raise(ag, EventDecremented, 1)
		},
	)
	assert.NoError(t, err)

	_, err = executor.Exec(ctx, timebox.NewAggregateID("engine", "x"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)

	select {
	case ev := <-consumer.Receive():
		assert.Equal(t, EventIncremented, ev.Type)
		assert.Equal(t, timebox.ID("flow"), ev.AggregateID[0])
	case <-time.After(eventWait):
		t.Fatal("timeout waiting for flow increment event")
	}

	select {
	case ev := <-consumer.Receive():
		t.Fatalf("unexpected event for %v", ev.AggregateID)
	case <-time.After(idleWait):
	}
}

func TestEventHubTypeOnly(t *testing.T) {
	const (
		eventWait = 1 * time.Second
		idleWait  = 100 * time.Millisecond
	)

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	consumer := tb.GetHub().NewTypeConsumer(
		EventIncremented,
	)
	defer consumer.Close()

	ctx := context.Background()

	_, err = executor.Exec(ctx, timebox.NewAggregateID("flow", "a"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)

	_, err = executor.Exec(ctx, timebox.NewAggregateID("engine", "x"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventDecremented, 1)
		},
	)
	assert.NoError(t, err)

	select {
	case ev := <-consumer.Receive():
		assert.Equal(t, EventIncremented, ev.Type)
	case <-time.After(eventWait):
		t.Fatal("timeout waiting for increment event")
	}

	select {
	case ev := <-consumer.Receive():
		t.Fatalf("unexpected event for %v", ev.AggregateID)
	case <-time.After(idleWait):
	}
}

func TestEventHubUnsubscribe(t *testing.T) {
	const (
		eventWait = 1 * time.Second
		closeWait = 1 * time.Second
	)

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	consumer := tb.GetHub().NewAggregateConsumer(
		timebox.NewAggregateID("flow"),
	)

	ctx := context.Background()

	_, err = executor.Exec(ctx, timebox.NewAggregateID("flow", "a"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)

	select {
	case <-consumer.Receive():
	case <-time.After(eventWait):
		t.Fatal("timeout waiting for event")
	}

	consumer.Close()

	_, err = executor.Exec(ctx, timebox.NewAggregateID("flow", "a"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)

	select {
	case _, ok := <-consumer.Receive():
		assert.False(t, ok)
	case <-time.After(closeWait):
		t.Fatal("timeout waiting for consumer close")
	}
}

func TestEventHubMultiplePrefixes(t *testing.T) {
	const eventWait = 1 * time.Second

	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	consumer := tb.GetHub().NewAggregatesConsumer(
		[]timebox.AggregateID{
			timebox.NewAggregateID("flow"),
			timebox.NewAggregateID("engine"),
		},
	)
	defer consumer.Close()

	ctx := context.Background()

	_, err = executor.Exec(ctx, timebox.NewAggregateID("flow", "a"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)

	_, err = executor.Exec(ctx, timebox.NewAggregateID("engine", "x"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)

	got := make([]timebox.ID, 0, 2)
	for range 2 {
		select {
		case ev := <-consumer.Receive():
			got = append(got, ev.AggregateID[0])
		case <-time.After(eventWait):
			t.Fatal("timeout waiting for event")
		}
	}
	assert.ElementsMatch(t, []timebox.ID{"flow", "engine"}, got)
}

func TestEventHubNoSubscribers(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(testStoreConfig(server.Addr(), nil))
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	ctx := context.Background()

	_, err = executor.Exec(ctx, timebox.NewAggregateID("flow", "a"),
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)
}

func TestStoreIndexer(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			server, tb, store, executor := setupTestExecutorWithConfig(
				t,
				func(cfg *timebox.Config) {
					cfg.Snapshot.TrimEvents = trimEvents
					cfg.Redis.Prefix = "store-indexer"
					cfg.Indexer = func(
						events []*timebox.Event,
					) []*timebox.Index {
						active := "active"
						return []*timebox.Index{{Status: &active}}
					}
				},
			)
			defer server.Close()
			defer func() { _ = store.Close() }()
			defer func() { _ = tb.Close() }()

			id := timebox.NewAggregateID("counter", "indexed")

			state, err := executor.Exec(
				context.Background(),
				id,
				func(
					s *CounterState, ag *timebox.Aggregator[*CounterState],
				) error {
					return timebox.Raise(ag, EventIncremented, 2)
				},
			)
			assert.NoError(t, err)
			assert.Equal(t, 2, state.Value)

			events, err := store.GetEvents(context.Background(), id, 0)
			assert.NoError(t, err)
			assert.Len(t, events, 1)
		})
	}
}

func newCounterState() *CounterState {
	return &CounterState{Value: 0}
}

func setupTestExecutor(t *testing.T) (
	*miniredis.Miniredis, *timebox.Timebox, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	return setupTestExecutorWithConfig(t, nil)
}

func setupTestExecutorWithConfig(
	t *testing.T, mutate func(*timebox.Config),
) (
	*miniredis.Miniredis, *timebox.Timebox, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	server, err := miniredis.Run()
	assert.NoError(t, err)

	tb, err := timebox.NewTimebox()
	assert.NoError(t, err)

	storeCfg := testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
		cfg.Redis.Prefix = "test"
	})
	if mutate != nil {
		mutate(&storeCfg)
	}

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, tb, store, executor
}

func testStoreConfig(addr string, mutate func(*timebox.Config)) timebox.Config {
	cfg := timebox.Config{
		Redis: timebox.RedisConfig{
			Addr: addr,
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

package timebox_test

import (
	"context"
	"encoding/json"
	"testing"

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

func TestSparseConfig(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	store, err := timebox.NewStore(
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

func TestStoreIndexer(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			server, store, executor := setupTestExecutorWithConfig(
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
	*miniredis.Miniredis, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	return setupTestExecutorWithConfig(t, nil)
}

func setupTestExecutorWithConfig(
	t *testing.T, mutate func(*timebox.Config),
) (
	*miniredis.Miniredis, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	server, err := miniredis.Run()
	assert.NoError(t, err)

	storeCfg := testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
		cfg.Redis.Prefix = "test"
	})
	if mutate != nil {
		mutate(&storeCfg)
	}

	store, err := timebox.NewStore(storeCfg)
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, store, executor
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

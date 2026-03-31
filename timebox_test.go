package timebox_test

import (
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
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
			server, store, executor := setupTestExecutorWithConfig(t,
				func(cfg *timebox.Config) {
					cfg.TrimEvents = trimEvents
					cfg.Indexer = func(
						events []*timebox.Event,
					) []*timebox.Index {
						active := "active"
						return []*timebox.Index{{Status: &active}}
					}
				},
			)
			defer func() { _ = server.Close() }()
			defer func() { _ = store.Close() }()

			id := timebox.NewAggregateID("counter", "indexed")

			state, err := executor.Exec(id, func(
				s *CounterState, ag *timebox.Aggregator[*CounterState],
			) error {
				return timebox.Raise(ag, EventIncremented, 2)
			})
			assert.NoError(t, err)
			assert.Equal(t, 2, state.Value)

			events, err := store.GetEvents(id, 0)
			assert.NoError(t, err)
			assert.Len(t, events, 1)
		})
	}
}

func newCounterState() *CounterState {
	return &CounterState{Value: 0}
}

func setupTestExecutor(t *testing.T) (
	io.Closer, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	return setupTestExecutorWithConfig(t, nil)
}

func newMemoryStore(cfg timebox.Config) (io.Closer, *timebox.Store, error) {
	p := memory.NewPersistence()
	s, err := p.NewStore(cfg)
	if err != nil {
		_ = p.Close()
		return nil, nil, err
	}
	return p, s, nil
}

func setupTestExecutorWithConfig(
	t *testing.T, mutate func(*timebox.Config),
) (
	io.Closer, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	storeCfg := timebox.Config{}
	if mutate != nil {
		mutate(&storeCfg)
	}

	p, store, err := newMemoryStore(storeCfg)
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return p, store, executor
}

func encodedSize(t *testing.T, value any) int {
	t.Helper()

	data, err := json.Marshal(value)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return len(data)
}

func eventsDataSize(evs []*timebox.Event) int {
	size := 0
	for _, ev := range evs {
		size += len(ev.Data)
	}
	return size
}

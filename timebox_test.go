package timebox_test

import (
	"encoding/json"
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

var appliers = timebox.Appliers[CounterState]{
	EventIncremented: func(st CounterState, ev *timebox.Event) CounterState {
		var delta int
		_ = json.Unmarshal(ev.Data, &delta)
		st.Value = st.Value + delta
		return st
	},
	EventDecremented: func(st CounterState, ev *timebox.Event) CounterState {
		var delta int
		_ = json.Unmarshal(ev.Data, &delta)
		st.Value = st.Value - delta
		return st
	},
	EventReset: func(st CounterState, ev *timebox.Event) CounterState {
		st.Value = 0
		return st
	},
}

func TestEventGetValue(t *testing.T) {
	type CachedData struct {
		Name string `json:"name"`
	}

	ev := &timebox.Event{
		Type: "event.cached",
		Data: []byte(`{"name":"cached"}`),
	}

	data, err := ev.GetValue[CachedData]()
	assert.NoError(t, err)
	assert.Equal(t, "cached", data.Name)

	values, err := ev.GetValue[map[string]any]()
	assert.NoError(t, err)
	assert.Equal(t, "cached", values["name"])

	ev.Data = []byte("not json")
	data, err = ev.GetValue[CachedData]()
	assert.NoError(t, err)
	assert.Equal(t, "cached", data.Name)

	_, err = ev.GetValue[map[string]any]()
	assert.Error(t, err)
}

func TestEventRaisedNotSerialized(t *testing.T) {
	ev := &timebox.Event{
		Type:   "event.raised",
		Data:   []byte(`{"value":1}`),
		Raised: true,
	}

	data, err := json.Marshal(ev)
	assert.NoError(t, err)

	var decoded map[string]any
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	_, ok := decoded["Raised"]
	assert.False(t, ok)
	_, ok = decoded["raised"]
	assert.False(t, ok)
}

func TestStoreIndexer(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			store, executor := setupTestExecutorWithConfig(t,
				func(cfg *timebox.Config) {
					cfg.TrimEvents = trimEvents
					cfg.Indexer = func(
						events []*timebox.Event,
					) []*timebox.Index {
						return []*timebox.Index{{Status: new("active")}}
					}
				},
			)
			defer func() { _ = store.Close() }()

			id := timebox.NewAggregateID("counter", "indexed")

			state, err := executor.Exec(id,
				func(
					st CounterState, ag *timebox.Aggregator[CounterState],
				) error {
					return ag.Raise(EventIncremented, 2)
				},
			)
			assert.NoError(t, err)
			assert.Equal(t, 2, state.Value)

			events, err := store.GetEvents(id, 0)
			assert.NoError(t, err)
			assert.Len(t, events, 1)
		})
	}
}

func newCounterState() CounterState {
	return CounterState{Value: 0}
}

func setupTestExecutor(t *testing.T) (
	*timebox.Store, *timebox.Executor[CounterState],
) {
	return setupTestExecutorWithConfig(t, nil)
}

func setupTestExecutorWithConfig(
	t *testing.T, mutate func(*timebox.Config),
) (
	*timebox.Store, *timebox.Executor[CounterState],
) {
	cfg := timebox.Config{}
	if mutate != nil {
		mutate(&cfg)
	}

	store, err := memory.NewStore(cfg)
	assert.NoError(t, err)

	executor := store.Executor(newCounterState, appliers)
	return store, executor
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

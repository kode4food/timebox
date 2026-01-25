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
	EventIncremented: func(state *CounterState, ev *timebox.Event) *CounterState {
		var delta int
		_ = json.Unmarshal(ev.Data, &delta)
		res := *state
		res.Value = state.Value + delta
		return &res
	},
	EventDecremented: func(state *CounterState, ev *timebox.Event) *CounterState {
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
	tb, err := timebox.NewTimebox(timebox.DefaultConfig())
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	ctx := tb.Context()
	assert.NotNil(t, ctx)
}

func TestEventHubNotification(t *testing.T) {
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

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
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

func newCounterState() *CounterState {
	return &CounterState{Value: 0}
}

func setupTestExecutor(t *testing.T) (
	*miniredis.Miniredis, *timebox.Timebox, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	return setupTestExecutorWithStoreConfig(t, nil)
}

func setupTestExecutorWithStoreConfig(
	t *testing.T, mutate func(*timebox.StoreConfig),
) (
	*miniredis.Miniredis, *timebox.Timebox, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	server, err := miniredis.Run()
	assert.NoError(t, err)

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "test"
	if mutate != nil {
		mutate(&storeCfg)
	}

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, tb, store, executor
}

package timebox_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func setupTestExecutor(t *testing.T) (
	*miniredis.Miniredis, *timebox.Timebox, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	server, err := miniredis.Run()
	require.NoError(t, err)

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "test"

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, tb, store, executor
}

func setupTestExecutorWithoutSnapshotWorker(t *testing.T) (
	*miniredis.Miniredis, *timebox.Timebox, *timebox.Store,
	*timebox.Executor[*CounterState],
) {
	server, err := miniredis.Run()
	require.NoError(t, err)

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "test"
	cfg.Workers = false

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, tb, store, executor
}

func newCounterState() *CounterState {
	return &CounterState{Value: 0}
}

// Integration tests

func TestTimeboxWithoutSnapshotWorker(t *testing.T) {
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

	require.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	err = executor.SaveSnapshot(ctx, id)
	require.NoError(t, err)
}

func TestEventHubNotification(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)
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

func TestSequenceInEventHub(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)
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
				t.Error("timeout waiting for event")
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
	require.NoError(t, err)

	receivedEvents := <-done
	require.Len(t, receivedEvents, 3)
	assert.Equal(t, int64(0), receivedEvents[0].Sequence)
	assert.Equal(t, int64(1), receivedEvents[1].Sequence)
	assert.Equal(t, int64(2), receivedEvents[2].Sequence)
}

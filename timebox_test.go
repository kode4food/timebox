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

// User-defined state (no NextSequence tracking required!)
type CounterState struct {
	Value int `json:"value"`
}

func newCounterState() *CounterState {
	return &CounterState{Value: 0}
}

const (
	EventIncremented timebox.EventType = "incremented"
	EventDecremented timebox.EventType = "decremented"
	EventReset       timebox.EventType = "reset"
)

// Appliers only care about business logic - no sequence tracking!
var appliers = map[timebox.EventType]timebox.Applier[*CounterState]{
	EventIncremented: func(state *CounterState, ev *timebox.Event) *CounterState {
		var delta int
		_ = json.Unmarshal(ev.Data, &delta)
		return &CounterState{Value: state.Value + delta}
	},
	EventDecremented: func(state *CounterState, ev *timebox.Event) *CounterState {
		var delta int
		_ = json.Unmarshal(ev.Data, &delta)
		return &CounterState{Value: state.Value - delta}
	},
	EventReset: func(state *CounterState, ev *timebox.Event) *CounterState {
		return &CounterState{Value: 0}
	},
}

func setupTestExecutor(t *testing.T) (*miniredis.Miniredis, *timebox.Executor[*CounterState]) {
	server, err := miniredis.Run()
	require.NoError(t, err)

	cfg := timebox.DefaultConfig()
	cfg.Store.Addr = server.Addr()
	cfg.Store.Prefix = "test"

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)

	executor := timebox.NewExecutor(tb, appliers, newCounterState)
	return server, executor
}

func setupTestExecutorWithoutSnapshotWorker(t *testing.T) (*miniredis.Miniredis, *timebox.Timebox, *timebox.Executor[*CounterState]) {
	server, err := miniredis.Run()
	require.NoError(t, err)

	cfg := timebox.DefaultConfig()
	cfg.Store.Addr = server.Addr()
	cfg.Store.Prefix = "test"
	cfg.EnableSnapshotWorker = false // Disable snapshot worker

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)

	executor := timebox.NewExecutor(tb, appliers, newCounterState)
	return server, tb, executor
}

func TestBasicIncrement(t *testing.T) {
	server, executor := setupTestExecutor(t)
	defer server.Close()
	defer executor.GetStore().Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "1")

	state, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(5)
		ag.Raise(EventIncremented, data)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 5, state.Value)
}

func TestTimeboxWithoutSnapshotWorker(t *testing.T) {
	server, tb, executor := setupTestExecutorWithoutSnapshotWorker(t)
	defer server.Close()
	defer tb.Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "no-snapshot")

	// Execute a command
	state, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(10)
		ag.Raise(EventIncremented, data)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	// Explicitly save snapshot
	err = executor.SaveSnapshot(ctx, id)
	require.NoError(t, err)
}

func TestMultipleOperations(t *testing.T) {
	server, executor := setupTestExecutor(t)
	defer server.Close()
	defer executor.GetStore().Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "1")

	// Increment
	state, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(10)
		ag.Raise(EventIncremented, data)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	// Increment again
	state, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		assert.Equal(t, 10, s.Value) // Previous state is loaded
		data, _ := json.Marshal(5)
		ag.Raise(EventIncremented, data)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 15, state.Value)

	// Decrement
	state, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(3)
		ag.Raise(EventDecremented, data)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 12, state.Value)

	// Reset
	state, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		ag.Raise(EventReset, json.RawMessage("{}"))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, state.Value)
}

func TestConcurrentWrites(t *testing.T) {
	server, executor := setupTestExecutor(t)
	defer server.Close()
	defer executor.GetStore().Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "concurrent")

	// Sequential writes (should all succeed due to optimistic concurrency)
	for i := 0; i < 10; i++ {
		_, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			data, _ := json.Marshal(1)
			ag.Raise(EventIncremented, data)
			return nil
		})
		require.NoError(t, err)
	}

	finalState, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 10, finalState.Value)
}

func TestEventHubNotification(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	cfg.Store.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)
	defer tb.Close()

	executor := timebox.NewExecutor(tb, appliers, newCounterState)
	consumer := tb.GetHub().NewConsumer()
	defer consumer.Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "notif")

	go func() {
		_, _ = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			data, _ := json.Marshal(1)
			ag.Raise(EventIncremented, data)
			return nil
		})
	}()

	select {
	case ev := <-consumer.Receive():
		assert.Equal(t, EventIncremented, ev.Type)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestAggregateID(t *testing.T) {
	id := timebox.NewAggregateID("counter", "123")
	assert.Len(t, id, 2)

	joined := id.Join(":")
	assert.Equal(t, "counter:123", joined)

	parsed := timebox.ParseAggregateID("counter:123", ":")
	assert.Equal(t, id, parsed)
}

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

func TestStoreOperations(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	cfg.Store.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)
	defer tb.Close()

	store := tb.GetStore()

	ctx := context.Background()
	id := timebox.NewAggregateID("test", "1")

	// Append events
	ev := &timebox.Event{
		ID:          "ev1",
		Type:        EventIncremented,
		AggregateID: id,
		Timestamp:   time.Now(),
		Sequence:    0,
		Data:        json.RawMessage("{}"),
	}

	err = store.AppendEvents(ctx, id, []*timebox.Event{ev})
	require.NoError(t, err)

	// Get events
	events, err := store.GetEvents(ctx, id, 0)
	require.NoError(t, err)
	assert.Len(t, events, 1)
}

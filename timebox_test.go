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

func newCounterState() *CounterState {
	return &CounterState{Value: 0}
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

func setupTestExecutor(t *testing.T) (*miniredis.Miniredis, *timebox.Timebox, *timebox.Store, *timebox.Executor[*CounterState]) {
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

	executor := timebox.NewExecutor(store, appliers, newCounterState)
	return server, tb, store, executor
}

func setupTestExecutorWithoutSnapshotWorker(t *testing.T) (*miniredis.Miniredis, *timebox.Timebox, *timebox.Store, *timebox.Executor[*CounterState]) {
	server, err := miniredis.Run()
	require.NoError(t, err)

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "test"
	cfg.EnableSnapshotWorker = false

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)

	executor := timebox.NewExecutor(store, appliers, newCounterState)
	return server, tb, store, executor
}

func TestBasicIncrement(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

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
	server, tb, store, executor := setupTestExecutorWithoutSnapshotWorker(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "no-snapshot")

	state, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(10)
		ag.Raise(EventIncremented, data)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	err = executor.SaveSnapshot(ctx, id)
	require.NoError(t, err)
}

func TestMultipleOperations(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "1")

	state, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(10)
		ag.Raise(EventIncremented, data)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	state, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		assert.Equal(t, 10, s.Value) // Previous state is loaded
		data, _ := json.Marshal(5)
		ag.Raise(EventIncremented, data)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 15, state.Value)

	state, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(3)
		ag.Raise(EventDecremented, data)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 12, state.Value)

	state, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		ag.Raise(EventReset, json.RawMessage("{}"))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, state.Value)
}

func TestConcurrentWrites(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "concurrent")

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
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, appliers, newCounterState)
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
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)
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
	require.NoError(t, err)

	events, err := store.GetEvents(ctx, id, 0)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, EventIncremented, events[0].Type)
}

func TestSequenceHandling(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "seq-test")

	// Test 1: Raise multiple events in single Exec - sequences should start at 0
	var capturedEvents []*timebox.Event
	_, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(1)
		ag.Raise(EventIncremented, data)
		ag.Raise(EventIncremented, data)
		ag.Raise(EventIncremented, data)
		capturedEvents = ag.Enqueued()
		return nil
	})
	require.NoError(t, err)

	// Verify sequences on raised events
	require.Len(t, capturedEvents, 3)
	assert.Equal(t, int64(0), capturedEvents[0].Sequence)
	assert.Equal(t, int64(1), capturedEvents[1].Sequence)
	assert.Equal(t, int64(2), capturedEvents[2].Sequence)

	// Test 2: Read events from storage - sequences should be populated
	events, err := store.GetEvents(ctx, id, 0)
	require.NoError(t, err)
	require.Len(t, events, 3)
	assert.Equal(t, int64(0), events[0].Sequence)
	assert.Equal(t, int64(1), events[1].Sequence)
	assert.Equal(t, int64(2), events[2].Sequence)

	// Test 3: Raise more events - sequences should continue from 3
	_, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		assert.Equal(t, int64(3), ag.NextSequence())
		data, _ := json.Marshal(1)
		ag.Raise(EventIncremented, data)
		ag.Raise(EventIncremented, data)
		capturedEvents = ag.Enqueued()
		return nil
	})
	require.NoError(t, err)

	require.Len(t, capturedEvents, 2)
	assert.Equal(t, int64(3), capturedEvents[0].Sequence)
	assert.Equal(t, int64(4), capturedEvents[1].Sequence)

	// Test 4: Read all events from storage
	allEvents, err := store.GetEvents(ctx, id, 0)
	require.NoError(t, err)
	require.Len(t, allEvents, 5)
	for i := 0; i < 5; i++ {
		assert.Equal(t, int64(i), allEvents[i].Sequence)
	}

	// Test 5: Read events from offset - sequences should still be correct
	partialEvents, err := store.GetEvents(ctx, id, 2)
	require.NoError(t, err)
	require.Len(t, partialEvents, 3)
	assert.Equal(t, int64(2), partialEvents[0].Sequence)
	assert.Equal(t, int64(3), partialEvents[1].Sequence)
	assert.Equal(t, int64(4), partialEvents[2].Sequence)
}

func TestSequenceWithSnapshot(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "snap-seq-test")

	// Raise some events
	_, err := executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(5)
		ag.Raise(EventIncremented, data)
		ag.Raise(EventIncremented, data)
		return nil
	})
	require.NoError(t, err)

	// Create snapshot at sequence 2
	err = executor.SaveSnapshot(ctx, id)
	require.NoError(t, err)

	// Raise more events after snapshot
	_, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(3)
		ag.Raise(EventIncremented, data)
		ag.Raise(EventIncremented, data)
		return nil
	})
	require.NoError(t, err)

	// Load snapshot and verify sequences on additional events
	var state CounterState
	snap, err := store.GetSnapshot(ctx, id, &state)
	require.NoError(t, err)
	assert.Equal(t, int64(2), snap.NextSequence)

	require.Len(t, snap.AdditionalEvents, 2)
	assert.Equal(t, int64(2), snap.AdditionalEvents[0].Sequence)
	assert.Equal(t, int64(3), snap.AdditionalEvents[1].Sequence)
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

	executor := timebox.NewExecutor(store, appliers, newCounterState)
	consumer := tb.GetHub().NewConsumer()
	defer consumer.Close()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "hub-seq-test")

	// Raise multiple events and verify sequences in event hub
	done := make(chan []*timebox.Event)
	go func() {
		var received []*timebox.Event
		for i := 0; i < 3; i++ {
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

	_, err = executor.Exec(ctx, id, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(1)
		ag.Raise(EventIncremented, data)
		ag.Raise(EventIncremented, data)
		ag.Raise(EventIncremented, data)
		return nil
	})
	require.NoError(t, err)

	receivedEvents := <-done
	require.Len(t, receivedEvents, 3)
	assert.Equal(t, int64(0), receivedEvents[0].Sequence)
	assert.Equal(t, int64(1), receivedEvents[1].Sequence)
	assert.Equal(t, int64(2), receivedEvents[2].Sequence)
}

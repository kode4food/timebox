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

func TestBasicIncrement(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "1")

	state, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 5)
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 5, state.Value)
}

func TestMultipleOperations(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "1")

	state, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 10)
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, 10, s.Value) // Previous state is loaded
			return timebox.Raise(ag, EventIncremented, 5)
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 15, state.Value)

	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventDecremented, 3)
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 12, state.Value)

	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventReset, struct{}{})
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 0, state.Value)
}

func TestConcurrentWrites(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "concurrent")

	for range 10 {
		_, err := executor.Exec(ctx, id,
			func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
				return timebox.Raise(ag, EventIncremented, 1)
			},
		)
		assert.NoError(t, err)
	}

	finalState, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 10, finalState.Value)
}

func TestSequenceHandling(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "seq-test")

	// Test 1: Raise multiple events in one Exec - sequences should start at 0
	var capturedEvents []*timebox.Event
	_, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			capturedEvents = ag.Enqueued()
			return nil
		},
	)
	assert.NoError(t, err)

	// Verify sequences on raised events
	assert.Len(t, capturedEvents, 3)
	assert.Equal(t, int64(0), capturedEvents[0].Sequence)
	assert.Equal(t, int64(1), capturedEvents[1].Sequence)
	assert.Equal(t, int64(2), capturedEvents[2].Sequence)

	// Test 2: Read events from storage - sequences should be populated
	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 3)
	assert.Equal(t, int64(0), events[0].Sequence)
	assert.Equal(t, int64(1), events[1].Sequence)
	assert.Equal(t, int64(2), events[2].Sequence)

	// Test 3: Raise more events - sequences should continue from 3
	_, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, int64(3), ag.NextSequence())
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			capturedEvents = ag.Enqueued()
			return nil
		},
	)
	assert.NoError(t, err)

	assert.Len(t, capturedEvents, 2)
	assert.Equal(t, int64(3), capturedEvents[0].Sequence)
	assert.Equal(t, int64(4), capturedEvents[1].Sequence)

	// Test 4: Read all events from storage
	allEvents, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, allEvents, 5)
	for i := range 5 {
		assert.Equal(t, int64(i), allEvents[i].Sequence)
	}

	// Test 5: Read events from offset - sequences should still be correct
	partialEvents, err := store.GetEvents(ctx, id, 2)
	assert.NoError(t, err)
	assert.Len(t, partialEvents, 3)
	assert.Equal(t, int64(2), partialEvents[0].Sequence)
	assert.Equal(t, int64(3), partialEvents[1].Sequence)
	assert.Equal(t, int64(4), partialEvents[2].Sequence)
}

func TestAppliesEvent(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	// Test with event types that have appliers
	incrementedEvent := &timebox.Event{Type: EventIncremented}
	assert.True(t, executor.AppliesEvent(incrementedEvent))

	decrementedEvent := &timebox.Event{Type: EventDecremented}
	assert.True(t, executor.AppliesEvent(decrementedEvent))

	resetEvent := &timebox.Event{Type: EventReset}
	assert.True(t, executor.AppliesEvent(resetEvent))

	// Test with event type that does not have an applier
	unknownEvent := &timebox.Event{Type: "unknown_event"}
	assert.False(t, executor.AppliesEvent(unknownEvent))
}

func TestConflictRetry(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	assert.Equal(t, store, executor.GetStore())

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "conflict")

	injected := false
	state, err := executor.Exec(ctx, id,
		func(_ *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			err := timebox.Raise(ag, EventIncremented, 1)
			if err != nil {
				return err
			}
			if injected {
				return nil
			}

			injected = true
			ev := &timebox.Event{
				Timestamp:   time.Now(),
				Type:        EventIncremented,
				AggregateID: id,
				Data:        json.RawMessage(`1`),
			}
			return store.AppendEvents(ctx, id, 0, []*timebox.Event{ev})
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, state.Value)

	_, err = executor.Exec(ctx, id,
		func(_ *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, id, ag.ID())
			return nil
		},
	)
	assert.NoError(t, err)
}

func TestCacheEviction(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	cfg.CacheSize = 1
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "evict"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	ctx := context.Background()

	id1 := timebox.NewAggregateID("counter", "1")
	id2 := timebox.NewAggregateID("counter", "2")

	state, err := executor.Exec(ctx, id1,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, state.Value)

	state, err = executor.Exec(ctx, id2,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return timebox.Raise(ag, EventIncremented, 2)
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, state.Value)
}

func TestCommandError(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "err")

	_, err := executor.Exec(ctx, id,
		func(_ *CounterState, _ *timebox.Aggregator[*CounterState]) error {
			return timebox.ErrUnexpectedLuaResult
		},
	)

	assert.Error(t, err)
}

func TestNoOpCommand(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "noop")

	state, err := executor.Exec(ctx, id,
		func(s *CounterState, _ *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, 0, s.Value)
			return nil
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 0, state.Value)
}

func TestRaiseError(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.AggregateID{"raise", "error"}
	_, err := executor.Exec(ctx, id, func(
		_ *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		ch := make(chan int)
		return timebox.Raise(ag, EventIncremented, ch)
	})

	assert.Error(t, err)
}

func TestZeroCacheSize(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	cfg.CacheSize = 0
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "zero-cache"

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	state, err := executor.Exec(
		context.Background(),
		timebox.NewAggregateID("counter", "1"),
		func(_ *CounterState, _ *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 0, state.Value)
}

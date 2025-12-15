package timebox_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			return ag.Raise(EventIncremented, 5)
		},
	)

	require.NoError(t, err)
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
			return ag.Raise(EventIncremented, 10)
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, 10, s.Value) // Previous state is loaded
			return ag.Raise(EventIncremented, 5)
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 15, state.Value)

	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return ag.Raise(EventDecremented, 3)
		},
	)
	require.NoError(t, err)
	assert.Equal(t, 12, state.Value)

	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return ag.Raise(EventReset, struct{}{})
		},
	)
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

	for range 10 {
		_, err := executor.Exec(ctx, id,
			func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
				return ag.Raise(EventIncremented, 1)
			},
		)
		require.NoError(t, err)
	}

	finalState, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)
	require.NoError(t, err)
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
			if err := ag.Raise(EventIncremented, 1); err != nil {
				return err
			}
			if err := ag.Raise(EventIncremented, 1); err != nil {
				return err
			}
			if err := ag.Raise(EventIncremented, 1); err != nil {
				return err
			}
			capturedEvents = ag.Enqueued()
			return nil
		},
	)
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
	_, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, int64(3), ag.NextSequence())
			if err := ag.Raise(EventIncremented, 1); err != nil {
				return err
			}
			if err := ag.Raise(EventIncremented, 1); err != nil {
				return err
			}
			capturedEvents = ag.Enqueued()
			return nil
		},
	)
	require.NoError(t, err)

	require.Len(t, capturedEvents, 2)
	assert.Equal(t, int64(3), capturedEvents[0].Sequence)
	assert.Equal(t, int64(4), capturedEvents[1].Sequence)

	// Test 4: Read all events from storage
	allEvents, err := store.GetEvents(ctx, id, 0)
	require.NoError(t, err)
	require.Len(t, allEvents, 5)
	for i := range 5 {
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
	_, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := ag.Raise(EventIncremented, 5); err != nil {
				return err
			}
			return ag.Raise(EventIncremented, 5)
		},
	)
	require.NoError(t, err)

	// Create snapshot at sequence 2
	err = executor.SaveSnapshot(ctx, id)
	require.NoError(t, err)

	// Raise more events after snapshot
	_, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := ag.Raise(EventIncremented, 3); err != nil {
				return err
			}
			return ag.Raise(EventIncremented, 3)
		},
	)
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

func TestLargeEventBatch(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "large-batch")

	// Test with more than 128 events to verify chunking works
	numEvents := 300

	state, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			for range numEvents {
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, numEvents, state.Value)

	// Verify all events were stored correctly
	events, err := store.GetEvents(ctx, id, 0)
	require.NoError(t, err)
	require.Len(t, events, numEvents)

	// Verify sequences are correct
	for i := range numEvents {
		assert.Equal(t, int64(i), events[i].Sequence)
	}

	// Test reading from an offset in the middle of the chunked data
	events, err = store.GetEvents(ctx, id, 150)
	require.NoError(t, err)
	require.Len(t, events, numEvents-150)
	assert.Equal(t, int64(150), events[0].Sequence)

	// Create a snapshot and verify it handles large event sets
	err = executor.SaveSnapshot(ctx, id)
	require.NoError(t, err)

	// Add more events after snapshot
	state, err = executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			for range 50 {
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, numEvents+50, state.Value)

	// Verify snapshot loading with additional events
	var snapState CounterState
	snap, err := store.GetSnapshot(ctx, id, &snapState)
	require.NoError(t, err)
	assert.Equal(t, numEvents, snapState.Value)
	require.Len(t, snap.AdditionalEvents, 50)
	for i := range 50 {
		assert.Equal(t, int64(numEvents+i), snap.AdditionalEvents[i].Sequence)
	}
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

package timebox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
	_, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			ag.OnSuccess(func(_ *CounterState, events []*timebox.Event) {
				assert.Len(t, events, 3)
				assert.Equal(t, int64(0), events[0].Sequence)
				assert.Equal(t, int64(1), events[1].Sequence)
				assert.Equal(t, int64(2), events[2].Sequence)
			})
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			return nil
		},
	)
	assert.NoError(t, err)

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
			ag.OnSuccess(func(_ *CounterState, events []*timebox.Event) {
				assert.Len(t, events, 2)
				assert.Equal(t, int64(3), events[0].Sequence)
				assert.Equal(t, int64(4), events[1].Sequence)
			})
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			return nil
		},
	)
	assert.NoError(t, err)

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

func TestMaxRetriesOverride(t *testing.T) {
	server, tb, store, executor := setupExecutorWithConfigs(
		t,
		timebox.Config{MaxRetries: 2},
		testStoreConfig("", func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "retry-override"
			cfg.MaxRetries = 1
		}),
	)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "retry-override")

	injected := false
	_, err := executor.Exec(ctx, id,
		func(_ *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if injected {
				return nil
			}
			injected = true
			return store.AppendEvents(ctx, id, 0, []*timebox.Event{
				{
					Timestamp:   time.Now(),
					Type:        EventIncremented,
					AggregateID: id,
					Data:        json.RawMessage(`1`),
				},
			})
		},
	)

	assert.ErrorIs(t, err, timebox.ErrMaxRetriesExceeded)
}

func TestMaxRetriesInherited(t *testing.T) {
	server, tb, store, executor := setupExecutorWithConfigs(
		t,
		timebox.Config{MaxRetries: 2},
		testStoreConfig("", func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "retry-inherited"
		}),
	)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "retry-inherited")

	injected := false
	state, err := executor.Exec(ctx, id,
		func(_ *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
			if injected {
				return nil
			}
			injected = true
			return store.AppendEvents(ctx, id, 0, []*timebox.Event{
				{
					Timestamp:   time.Now(),
					Type:        EventIncremented,
					AggregateID: id,
					Data:        json.RawMessage(`1`),
				},
			})
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 2, state.Value)
}

func TestCacheEviction(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	tb, err := timebox.NewTimebox(timebox.Config{
		CacheSize: 1,
	})
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(
		testStoreConfig(server.Addr(), func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "evict"
		}),
	)
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

func TestCacheSizeOverride(t *testing.T) {
	server, tb, store, executor, count := setupExecutorWithCacheConfigs(
		t,
		timebox.Config{CacheSize: 2},
		testStoreConfig("", func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "cache-override"
			cfg.CacheSize = 1
		}),
	)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	assertCacheEviction(t, executor, count)
}

func TestCacheSizeInherited(t *testing.T) {
	server, tb, store, executor, count := setupExecutorWithCacheConfigs(
		t,
		timebox.Config{CacheSize: 1},
		testStoreConfig("", func(cfg *timebox.Config) {
			cfg.Redis.Prefix = "cache-inherited"
		}),
	)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	assertCacheEviction(t, executor, count)
}

func TestCommandError(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "err")

	_, err := executor.Exec(ctx, id,
		func(*CounterState, *timebox.Aggregator[*CounterState]) error {
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
	err := (timebox.Config{MaxRetries: 1}).Validate()
	assert.ErrorIs(t, err, timebox.ErrInvalidCacheSize)
}

func TestOnSuccessCallbacks(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	prev := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(prev)

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "on-success")

	var called []int
	var values []int
	var eventTypes []timebox.EventType
	var eventCounts []int
	_, err := executor.Exec(ctx, id,
		func(_ *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			ag.OnSuccess(func(state *CounterState, evs []*timebox.Event) {
				called = append(called, 1)
				values = append(values, state.Value)
				eventCounts = append(eventCounts, len(evs))
				if assert.Len(t, evs, 1) {
					eventTypes = append(eventTypes, evs[0].Type)
					assert.Equal(t, id, evs[0].AggregateID)
					assert.Equal(t, int64(0), evs[0].Sequence)
				}
			})
			ag.OnSuccess(func(state *CounterState, evs []*timebox.Event) {
				called = append(called, 2)
				values = append(values, state.Value)
				eventCounts = append(eventCounts, len(evs))
				if assert.Len(t, evs, 1) {
					eventTypes = append(eventTypes, evs[0].Type)
				}
				panic("boom")
			})
			ag.OnSuccess(func(state *CounterState, evs []*timebox.Event) {
				called = append(called, 3)
				values = append(values, state.Value)
				eventCounts = append(eventCounts, len(evs))
				if assert.Len(t, evs, 1) {
					eventTypes = append(eventTypes, evs[0].Type)
				}
			})
			return timebox.Raise(ag, EventIncremented, 1)
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, called)
	assert.Equal(t, []int{1, 1, 1}, values)
	assert.Equal(t, []int{1, 1, 1}, eventCounts)
	assert.Equal(t,
		[]timebox.EventType{
			EventIncremented, EventIncremented, EventIncremented,
		},
		eventTypes,
	)
	assert.Contains(t, buf.String(), "OnSuccess action panicked")
}

func TestOnSuccessNoOp(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "on-success-noop")

	called := false
	state, err := executor.Exec(ctx, id,
		func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			assert.Equal(t, 0, s.Value)
			ag.OnSuccess(func(state *CounterState, evs []*timebox.Event) {
				called = true
				assert.Equal(t, 0, state.Value)
				assert.Empty(t, evs)
			})
			return nil
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 0, state.Value)
	assert.True(t, called)
}

func TestOnSuccessNotCalledOnError(t *testing.T) {
	server, tb, store, executor := setupTestExecutor(t)
	defer server.Close()
	defer func() { _ = tb.Close() }()
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "on-success-error")

	called := false
	_, err := executor.Exec(ctx, id,
		func(_ *CounterState, ag *timebox.Aggregator[*CounterState]) error {
			ag.OnSuccess(func(*CounterState, []*timebox.Event) {
				called = true
			})
			return errors.New("nope")
		},
	)

	assert.Error(t, err)
	assert.False(t, called)
}

func assertCacheEviction(
	t *testing.T,
	executor *timebox.Executor[*CounterState],
	count *int,
) {
	t.Helper()

	ctx := context.Background()
	id1 := timebox.NewAggregateID("counter", "1")
	id2 := timebox.NewAggregateID("counter", "2")

	_, err := executor.Exec(ctx, id1,
		func(*CounterState, *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 2, *count)

	_, err = executor.Exec(ctx, id2,
		func(*CounterState, *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 4, *count)

	_, err = executor.Exec(ctx, id1,
		func(*CounterState, *timebox.Aggregator[*CounterState]) error {
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 6, *count)
}

func setupExecutorWithCacheConfigs(
	t *testing.T,
	tbCfg timebox.Config,
	storeCfg timebox.Config,
) (
	*miniredis.Miniredis,
	*timebox.Timebox,
	*timebox.Store,
	*timebox.Executor[*CounterState],
	*int,
) {
	t.Helper()

	server, tb, store := setupExecutorStore(t, tbCfg, storeCfg)
	count := 0
	executor := timebox.NewExecutor(store, func() *CounterState {
		count++
		return newCounterState()
	}, appliers)
	return server, tb, store, executor, &count
}

func setupExecutorWithConfigs(
	t *testing.T,
	tbCfg timebox.Config,
	storeCfg timebox.Config,
) (
	*miniredis.Miniredis,
	*timebox.Timebox,
	*timebox.Store,
	*timebox.Executor[*CounterState],
) {
	t.Helper()

	server, tb, store := setupExecutorStore(t, tbCfg, storeCfg)
	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, tb, store, executor
}

func setupExecutorStore(
	t *testing.T,
	tbCfg timebox.Config,
	storeCfg timebox.Config,
) (*miniredis.Miniredis, *timebox.Timebox, *timebox.Store) {
	t.Helper()

	server, err := miniredis.Run()
	assert.NoError(t, err)

	tb, err := timebox.NewTimebox(tbCfg)
	assert.NoError(t, err)

	if storeCfg.Redis.Addr == "" {
		storeCfg.Redis.Addr = server.Addr()
	}
	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)

	return server, tb, store
}

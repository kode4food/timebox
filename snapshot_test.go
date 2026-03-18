package timebox_test

import (
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

type snapshotGatePersistence struct {
	timebox.AlwaysReady
	canSave   bool
	saveCh    chan struct{}
	saveErr   error
	startedCh chan struct{} // signaled when SaveSnapshot starts
	blockCh   chan struct{} // SaveSnapshot blocks until this is closed
	mu        sync.Mutex
}

func (p *snapshotGatePersistence) Close() error {
	return nil
}

func (p *snapshotGatePersistence) Append(
	timebox.AppendRequest,
) (*timebox.AppendResult, error) {
	return nil, nil
}

func (p *snapshotGatePersistence) LoadEvents(
	timebox.AggregateID, int64,
) (*timebox.EventsResult, error) {
	return &timebox.EventsResult{}, nil
}

func (p *snapshotGatePersistence) LoadSnapshot(
	timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	return &timebox.SnapshotRecord{
		Events: []json.RawMessage{snapshotGateEvent(1)},
	}, nil
}

func (p *snapshotGatePersistence) SaveSnapshot(
	timebox.AggregateID, []byte, int64,
) error {
	if p.startedCh != nil {
		select {
		case p.startedCh <- struct{}{}:
		default:
		}
	}
	if p.blockCh != nil {
		<-p.blockCh
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case p.saveCh <- struct{}{}:
	default:
	}
	return p.saveErr
}

func (p *snapshotGatePersistence) ListAggregates(
	timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	return nil, nil
}

func (p *snapshotGatePersistence) GetAggregateStatus(
	timebox.AggregateID,
) (string, error) {
	return "", nil
}

func (p *snapshotGatePersistence) ListAggregatesByStatus(
	string,
) ([]timebox.StatusEntry, error) {
	return nil, nil
}

func (p *snapshotGatePersistence) ListAggregatesByLabel(
	string, string,
) ([]timebox.AggregateID, error) {
	return nil, nil
}

func (p *snapshotGatePersistence) ListLabelValues(string) ([]string, error) {
	return nil, nil
}

func (p *snapshotGatePersistence) CanSaveSnapshot() bool {
	return p.canSave
}

func TestWithoutSnapshotWorker(t *testing.T) {
	server, store, executor := setupTestExecutorWithoutSnapshotWorker(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "no-snapshot")

	state, err := executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		return timebox.Raise(ag, EventIncremented, 10)
	})

	assert.NoError(t, err)
	assert.Equal(t, 10, state.Value)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)
}

func TestSequenceWithSnapshot(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "snap-seq-test")

	_, err := executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		if err := timebox.Raise(ag, EventIncremented, 5); err != nil {
			return err
		}
		return timebox.Raise(ag, EventIncremented, 5)
	})
	assert.NoError(t, err)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	_, err = executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		if err := timebox.Raise(ag, EventIncremented, 3); err != nil {
			return err
		}
		return timebox.Raise(ag, EventIncremented, 3)
	})
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(id, &state)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), snap.NextSequence)

	assert.Len(t, snap.AdditionalEvents, 2)
	assert.Equal(t, int64(2), snap.AdditionalEvents[0].Sequence)
	assert.Equal(t, int64(3), snap.AdditionalEvents[1].Sequence)
}

func TestSnapshotTrimsEvents(t *testing.T) {
	server, store, executor := setupTestExecutorWithConfig(
		t,
		func(cfg *timebox.Config) {
			cfg.Snapshot.TrimEvents = true
		},
	)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "trim-events")

	_, err := executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		for range 3 {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 0)

	aggregates, err := store.ListAggregates(id)
	assert.NoError(t, err)
	assert.Len(t, aggregates, 1)
	assert.Equal(t, id, aggregates[0])

	_, err = executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		for range 2 {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)

	events, err = store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, int64(3), events[0].Sequence)
	assert.Equal(t, int64(4), events[1].Sequence)
}

func TestSnapshotLargeBatch(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "large-batch")

	numEvents := 300

	state, err := executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		for range numEvents {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, numEvents, state.Value)

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, numEvents)

	for i := range numEvents {
		assert.Equal(t, int64(i), events[i].Sequence)
	}

	events, err = store.GetEvents(id, 150)
	assert.NoError(t, err)
	assert.Len(t, events, numEvents-150)
	assert.Equal(t, int64(150), events[0].Sequence)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	state, err = executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		for range 50 {
			if err := timebox.Raise(ag, EventIncremented, 1); err != nil {
				return err
			}
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, numEvents+50, state.Value)

	var snapState CounterState
	snap, err := store.GetSnapshot(id, &snapState)
	assert.NoError(t, err)
	assert.Equal(t, numEvents, snapState.Value)
	assert.Len(t, snap.AdditionalEvents, 50)
	for i := range 50 {
		assert.Equal(t, int64(numEvents+i), snap.AdditionalEvents[i].Sequence)
	}
}

func TestSnapshotWorker(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{
		Snapshot: timebox.SnapshotConfig{
			Workers:      true,
			WorkerCount:  1,
			MaxQueueSize: 1,
		},
	})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "snapshot")
	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`1`),
	}
	err = store.AppendEvents(id, 0, []*timebox.Event{ev})
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	_, err = executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		assert.Equal(t, 1, s.Value)
		return nil
	})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		var snap CounterState
		result, err := store.GetSnapshot(id, &snap)
		if err != nil || result == nil {
			return false
		}
		return result.NextSequence > 0
	}, time.Second, 10*time.Millisecond)
}

func TestSaveSnapshotError(t *testing.T) {
	server, store, _ := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	type BadState struct {
		Value chan int
	}

	executor := timebox.NewExecutor(
		store,
		func() *BadState { return &BadState{Value: make(chan int)} },
		timebox.Appliers[*BadState]{},
	)

	err := executor.SaveSnapshot(timebox.NewAggregateID("bad", "snapshot"))
	assert.Error(t, err)
}

func TestSaveSnapshot(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("save", "snapshot")

	_, err := executor.Exec(id, func(
		s *CounterState, ag *timebox.Aggregator[*CounterState],
	) error {
		assert.Equal(t, 0, s.Value)
		return timebox.Raise(ag, EventIncremented, 2)
	})
	assert.NoError(t, err)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(id, &state)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), snap.NextSequence)
	assert.Equal(t, 2, state.Value)
}

func TestSnapshotWorkerSkipsPersistenceThatCannotSave(t *testing.T) {
	p := &snapshotGatePersistence{
		saveCh: make(chan struct{}, 1),
	}
	store := newSnapshotGateStore(t, p)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	_, err := executor.Exec(timebox.NewAggregateID("counter", "no-auto"), func(
		*CounterState, *timebox.Aggregator[*CounterState],
	) error {
		return nil
	})
	assert.NoError(t, err)

	assert.Never(t, func() bool {
		select {
		case <-p.saveCh:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)
}

func TestSnapshotWorkerRunsWhenPersistenceCanSave(t *testing.T) {
	p := &snapshotGatePersistence{
		canSave: true,
		saveCh:  make(chan struct{}, 1),
	}
	store := newSnapshotGateStore(t, p)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	_, err := executor.Exec(timebox.NewAggregateID("counter", "auto"), func(
		*CounterState, *timebox.Aggregator[*CounterState],
	) error {
		return nil
	})
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		select {
		case <-p.saveCh:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestSnapshotWorkerSaveError(t *testing.T) {
	saveErr := errors.New("storage full")
	p := &snapshotGatePersistence{
		canSave: true,
		saveCh:  make(chan struct{}, 1),
		saveErr: saveErr,
	}
	store := newSnapshotGateStore(t, p)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	_, err := executor.Exec(
		timebox.NewAggregateID("counter", "save-error"),
		func(
			*CounterState, *timebox.Aggregator[*CounterState],
		) error {
			return nil
		},
	)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		select {
		case <-p.saveCh:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestSnapshotWorkerQueueFull(t *testing.T) {
	blockCh := make(chan struct{})
	p := &snapshotGatePersistence{
		canSave:   true,
		saveCh:    make(chan struct{}, 3),
		startedCh: make(chan struct{}, 1),
		blockCh:   blockCh,
	}
	store := newSnapshotGateStore(t, p)
	t.Cleanup(func() { close(blockCh) })

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	id := timebox.NewAggregateID("counter", "queue-full")

	_, err := executor.Exec(id, func(
		*CounterState, *timebox.Aggregator[*CounterState],
	) error {
		return nil
	})
	assert.NoError(t, err)

	// Wait until the worker has dequeued the first item and is blocking
	<-p.startedCh

	// Fill the queue (1/1) then overflow it — enqueue drops the third
	_, err = executor.Exec(id, func(
		*CounterState, *timebox.Aggregator[*CounterState],
	) error {
		return nil
	})
	assert.NoError(t, err)
	_, err = executor.Exec(id, func(
		*CounterState, *timebox.Aggregator[*CounterState],
	) error {
		return nil
	})
	assert.NoError(t, err)
}

func TestSaveSnapshotLoadError(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("save", "snapshot-error")

	_ = server.Close()

	err := executor.SaveSnapshot(id)
	assert.Error(t, err)
}

func TestGetSnapshotEmpty(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	var state CounterState
	snap, err := store.GetSnapshot(
		timebox.NewAggregateID("counter", "1"), &state,
	)
	assert.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Len(t, snap.AdditionalEvents, 0)
	assert.Equal(t, int64(0), snap.NextSequence)
}

func setupTestExecutorWithoutSnapshotWorker(t *testing.T) (
	io.Closer, *timebox.Store, *timebox.Executor[*CounterState],
) {
	server, store, err := newMemoryStore(timebox.Config{})
	assert.NoError(t, err)

	executor := timebox.NewExecutor(store, newCounterState, appliers)
	return server, store, executor
}

func newSnapshotGateStore(
	t *testing.T, p *snapshotGatePersistence,
) *timebox.Store {
	t.Helper()
	store, err := timebox.NewStore(p, timebox.Config{
		Snapshot: timebox.SnapshotConfig{
			Workers:      true,
			WorkerCount:  1,
			MaxQueueSize: 1,
		},
	})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func snapshotGateEvent(delta int) json.RawMessage {
	data, _ := json.Marshal(map[string]any{
		"timestamp": time.Now().UTC(),
		"type":      EventIncremented,
		"data":      delta,
	})
	return data
}

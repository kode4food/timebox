package timebox_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
)

type inlineSnapshotBackend struct {
	fakeBackend
	saveErr      error
	rec          *timebox.SnapshotRecord
	saveData     []byte
	saveID       timebox.AggregateID
	saveSequence int64
	saveCount    int
}

func TestSequenceWithSnapshot(t *testing.T) {
	store, executor := setupTestExecutor(t)

	id := timebox.NewAggregateID("counter", "snap-seq-test")

	_, err := executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			if err := ag.Raise(EventIncremented, 5); err != nil {
				return err
			}
			return ag.Raise(EventIncremented, 5)
		},
	)
	assert.NoError(t, err)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	_, err = executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			if err := ag.Raise(EventIncremented, 3); err != nil {
				return err
			}
			return ag.Raise(EventIncremented, 3)
		},
	)
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(id, &state)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), snap.NextSequence)
	assert.Equal(t, encodedSize(t, &CounterState{Value: 10}), snap.SnapshotSize)
	assert.Equal(t, eventsDataSize(snap.AdditionalEvents), snap.EventsSize)

	assert.Len(t, snap.AdditionalEvents, 2)
	assert.Equal(t, int64(2), snap.AdditionalEvents[0].Sequence)
	assert.Equal(t, int64(3), snap.AdditionalEvents[1].Sequence)
}

func TestSnapshotTrimsEvents(t *testing.T) {
	store, executor := setupTestExecutorWithConfig(t,
		func(cfg *timebox.Config) {
			cfg.TrimEvents = true
		},
	)

	id := timebox.NewAggregateID("counter", "trim-events")

	_, err := executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			for range 3 {
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	assert.NoError(t, err)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 0)

	aggregates, err := store.ListAggregates(id.Type)
	assert.NoError(t, err)
	assert.Len(t, aggregates, 1)
	assert.Equal(t, id, aggregates[0])

	_, err = executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			for range 2 {
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	assert.NoError(t, err)

	events, err = store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, int64(3), events[0].Sequence)
	assert.Equal(t, int64(4), events[1].Sequence)
}

func TestSnapshotLargeBatch(t *testing.T) {
	store, executor := setupTestExecutor(t)

	id := timebox.NewAggregateID("counter", "large-batch")

	numEvents := 300

	state, err := executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			for range numEvents {
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
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

	state, err = executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			for range 50 {
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
			}
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, numEvents+50, state.Value)

	var snapState CounterState
	snap, err := store.GetSnapshot(id, &snapState)
	assert.NoError(t, err)
	assert.Equal(t, numEvents, snapState.Value)
	assert.Equal(t,
		encodedSize(t, &CounterState{Value: numEvents}), snap.SnapshotSize,
	)
	assert.Equal(t, eventsDataSize(snap.AdditionalEvents), snap.EventsSize)
	assert.Len(t, snap.AdditionalEvents, 50)
	for i := range 50 {
		assert.Equal(t, int64(numEvents+i), snap.AdditionalEvents[i].Sequence)
	}
}

func TestExecSnapshotsInlineWhenRatioExceeded(t *testing.T) {
	store, err := memory.Open().NewStore(timebox.Config{})
	assert.NoError(t, err)

	id := timebox.NewAggregateID("counter", "snapshot")
	ev := &timebox.Event{
		Timestamp: time.Unix(1_700_000_021, 0).UTC(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`1`),
	}
	err = store.AppendEvents(id, 0, []*timebox.Event{ev})
	assert.NoError(t, err)

	executor := store.Executor(newCounterState, appliers)
	state, err := executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			assert.Equal(t, 1, st.Value)
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, state.Value)

	var snapState CounterState
	snap, err := store.GetSnapshot(id, &snapState)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), snap.NextSequence)
	assert.Equal(t, encodedSize(t, &CounterState{Value: 1}), snap.SnapshotSize)
	assert.Equal(t, 0, snap.EventsSize)
	assert.Len(t, snap.AdditionalEvents, 0)
	assert.Equal(t, 1, snapState.Value)
}

func TestExecDoesNotSnapshotInlineBelowRatio(t *testing.T) {
	store, setupExecutor := setupTestExecutorWithConfig(t,
		func(cfg *timebox.Config) {
			cfg.SnapshotRatio = 100
		},
	)

	id := timebox.NewAggregateID("counter", "snapshot-ratio")
	_, err := setupExecutor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			return ag.Raise(EventIncremented, 10)
		},
	)
	assert.NoError(t, err)

	err = setupExecutor.SaveSnapshot(id)
	assert.NoError(t, err)

	ev := &timebox.Event{
		Timestamp: time.Unix(1_700_000_022, 0).UTC(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`1`),
	}
	err = store.AppendEvents(id, 1, []*timebox.Event{ev})
	assert.NoError(t, err)

	executor := store.Executor(newCounterState, appliers)
	state, err := executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			assert.Equal(t, 11, st.Value)
			return nil
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, 11, state.Value)

	var snapState CounterState
	snap, err := store.GetSnapshot(id, &snapState)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), snap.NextSequence)
	assert.Equal(t, encodedSize(t, &CounterState{Value: 10}), snap.SnapshotSize)
	assert.Equal(t, eventsDataSize(snap.AdditionalEvents), snap.EventsSize)
	assert.Len(t, snap.AdditionalEvents, 1)
	assert.Equal(t, int64(1), snap.AdditionalEvents[0].Sequence)
	assert.Equal(t, 10, snapState.Value)
}

func TestSaveSnapshotError(t *testing.T) {
	store, _ := setupTestExecutor(t)

	type BadState struct {
		Value chan int
	}

	executor := store.Executor(
		func() *BadState { return &BadState{Value: make(chan int)} },
		timebox.Appliers[*BadState]{},
	)

	err := executor.SaveSnapshot(timebox.NewAggregateID("bad", "snapshot"))
	assert.Error(t, err)
}

func TestSaveSnapshot(t *testing.T) {
	store, executor := setupTestExecutor(t)

	id := timebox.NewAggregateID("save", "snapshot")

	_, err := executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			assert.Equal(t, 0, st.Value)
			return ag.Raise(EventIncremented, 2)
		},
	)
	assert.NoError(t, err)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(id, &state)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), snap.NextSequence)
	assert.Equal(t, encodedSize(t, &CounterState{Value: 2}), snap.SnapshotSize)
	assert.Equal(t, 0, snap.EventsSize)
	assert.Equal(t, 2, state.Value)
}

func TestSaveSnapshotColdCache(t *testing.T) {
	store, executor := setupTestExecutor(t)

	id := timebox.NewAggregateID("counter", "cold-snapshot")

	_, err := executor.Exec(id,
		func(_ CounterState, ag *timebox.Aggregator[CounterState]) error {
			return ag.Raise(EventIncremented, 5)
		},
	)
	assert.NoError(t, err)

	err = executor.SaveSnapshot(id)
	assert.NoError(t, err)

	// Append events directly, bypassing the executor cache
	err = store.AppendEvents(id, 1, []*timebox.Event{
		{
			Timestamp: time.Unix(1_700_000_030, 0).UTC(),
			Type:      EventIncremented,
			Data:      json.RawMessage(`3`),
		},
	})
	assert.NoError(t, err)

	// Fresh executor with cold cache must fast-forward through the new event
	fresh := store.Executor(newCounterState, appliers)
	err = fresh.SaveSnapshot(id)
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(id, &state)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), snap.NextSequence)
	assert.Equal(t, 8, state.Value)
	assert.Empty(t, snap.AdditionalEvents)
}

func TestSaveSnapshotLoadError(t *testing.T) {
	b := memory.Open()
	store, err := b.NewStore()
	assert.NoError(t, err)

	executor := store.Executor(newCounterState, appliers)
	id := timebox.NewAggregateID("save", "snapshot-error")

	_ = b.Close()

	assert.Error(t, executor.SaveSnapshot(id))
}

func TestGetSnapshotEmpty(t *testing.T) {
	store, err := memory.Open().NewStore(timebox.Config{})
	assert.NoError(t, err)

	var state CounterState
	snap, err := store.GetSnapshot(
		timebox.NewAggregateID("counter", "1"), &state,
	)
	assert.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Len(t, snap.AdditionalEvents, 0)
	assert.Equal(t, int64(0), snap.NextSequence)
	assert.Equal(t, 0, snap.SnapshotSize)
	assert.Equal(t, 0, snap.EventsSize)
}

func TestExecInlineSnapshotSaveError(t *testing.T) {
	saveErr := errors.New("snapshot save failed")
	recData, err := json.Marshal(&CounterState{Value: 10})
	assert.NoError(t, err)

	b := &inlineSnapshotBackend{
		rec: &timebox.SnapshotRecord{
			Data:     recData,
			Sequence: 1,
			Events: []*timebox.Event{
				inlineSnapshotEvent(t, 1_000_000),
				inlineSnapshotEvent(t, 1_000_000),
			},
		},
		saveErr: saveErr,
	}
	store, err := timebox.NewStore(b)
	assert.NoError(t, err)

	executor := store.Executor(newCounterState, appliers)
	id := timebox.NewAggregateID("counter", "inline-save-error")

	_, err = executor.Exec(id,
		func(st CounterState, ag *timebox.Aggregator[CounterState]) error {
			assert.Equal(t, 2_000_010, st.Value)
			return nil
		},
	)
	assert.ErrorIs(t, err, saveErr)
	assert.Equal(t, 1, b.saveCount)
	assert.Equal(t, id, b.saveID)
	assert.Equal(t, int64(3), b.saveSequence)

	var saved CounterState
	err = json.Unmarshal(b.saveData, &saved)
	assert.NoError(t, err)
	assert.Equal(t, 2_000_010, saved.Value)
}

func inlineSnapshotEvent(t *testing.T, delta int) *timebox.Event {
	t.Helper()

	data, err := json.Marshal(delta)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return &timebox.Event{
		Timestamp: time.Unix(1_700_000_023, 0).UTC(),
		Type:      EventIncremented,
		Data:      data,
	}
}

func (b *inlineSnapshotBackend) LoadSnapshot(
	timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	return &timebox.SnapshotRecord{
		Data:     append([]byte(nil), b.rec.Data...),
		Sequence: b.rec.Sequence,
		Events:   append([]*timebox.Event(nil), b.rec.Events...),
	}, nil
}

func (b *inlineSnapshotBackend) SaveSnapshot(
	req timebox.SnapshotRequest,
) error {
	b.saveID = req.ID
	b.saveData = append([]byte(nil), req.Data...)
	b.saveSequence = req.Sequence
	b.saveCount++
	return b.saveErr
}

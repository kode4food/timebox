package raft_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestAppend(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "append")
	firstTS := time.Unix(1_700_000_000, 0).UTC()
	secondTS := firstTS.Add(time.Minute)

	err := n.store.AppendEvents(id, 0, []*timebox.Event{
		indexedEvent(id, "active", "prod", firstTS),
		indexedEvent(id, "active", "prod", secondTS),
	})
	if !assert.NoError(t, err) {
		return
	}

	evs, err := n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, evs, 2)
	assert.Equal(t, int64(0), evs[0].Sequence)
	assert.Equal(t, int64(1), evs[1].Sequence)

	statuses, err := n.store.ListAggregatesByStatus("active")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        id,
		Timestamp: secondTS,
	}}, statuses)

	ids, err := n.store.ListAggregatesByLabel("env", "prod")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{id}, ids)

	err = n.store.AppendEvents(id, 1, []*timebox.Event{
		indexedEvent(id, "paused", "stage", secondTS.Add(time.Minute)),
	})
	if !assert.Error(t, err) {
		return
	}

	var conflict *timebox.VersionConflictError
	if !assert.ErrorAs(t, err, &conflict) {
		return
	}
	assert.Equal(t, int64(2), conflict.ActualSequence)
	assert.Len(t, conflict.NewEvents, 1)
	assert.Equal(t, int64(1), conflict.NewEvents[0].Sequence)
}

func TestAppendClosed(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	p := n.persistence
	closeNode(t, n)

	id := timebox.NewAggregateID("order", "append-closed")
	err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           []*timebox.Event{numberEvent(id, 1)},
	})
	assert.Error(t, err)
}

func TestSnapshotFutureSeq(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "future-snap")
	err := n.store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
	if !assert.NoError(t, err) {
		return
	}

	err = n.store.PutSnapshot(id, map[string]int{"value": 5}, 5)
	if !assert.NoError(t, err) {
		return
	}

	err = n.store.AppendEvents(id, 5, []*timebox.Event{numberEvent(id, 6)})
	if !assert.NoError(t, err) {
		return
	}

	var snapState map[string]int
	snap, err := n.store.GetSnapshot(id, &snapState)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, map[string]int{"value": 5}, snapState)
	assert.Equal(t, int64(5), snap.NextSequence)
	assert.Len(t, snap.AdditionalEvents, 1)
	assert.Equal(t, int64(5), snap.AdditionalEvents[0].Sequence)
}

// TestConcurrentAppend verifies that when concurrent appends for the same
// aggregate all pass the pre-check and reach the FSM, only one commits and
// the rest receive a VersionConflictError
func TestConcurrentAppend(t *testing.T) {
	n := newNode(t, nodeConfig{id: "node-1"})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "concurrent")
	const workers = 10

	// All goroutines wait at the barrier so they checkConflict and
	// propose simultaneously, before any raft commit can complete
	var ready sync.WaitGroup
	ready.Add(workers)
	start := make(chan struct{})
	errs := make(chan error, workers)

	for i := range workers {
		go func(i int) {
			ready.Done()
			<-start
			errs <- n.store.AppendEvents(id, 0, []*timebox.Event{
				numberEvent(id, i+1),
			})
		}(i)
	}

	ready.Wait()
	close(start)

	var successes int
	for range workers {
		err := <-errs
		if err == nil {
			successes++
		} else {
			var conflict *timebox.VersionConflictError
			assert.ErrorAs(t, err, &conflict)
		}
	}
	assert.Equal(t, 1, successes)
}

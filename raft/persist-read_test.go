package raft_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestLoadEventsUnknownAggregate(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "missing")
	res, err := n.persistence.LoadEvents(id, 7)
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, int64(7), res.StartSequence)
	assert.Empty(t, res.Events)
}

func TestLoadEventsUsesBaseSequenceAfterTrim(t *testing.T) {
	n := newNode(t, nodeConfig{
		id:         "node-1",
		trimEvents: true,
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "trimmed-events")
	appendN(t, n.store, id, 3)

	err := n.store.PutSnapshot(id, map[string]int{"value": 2}, 2)
	if !assert.NoError(t, err) {
		return
	}
	err = n.store.AppendEvents(id, 3, []*timebox.Event{
		numberEvent(id, 4),
	})
	if !assert.NoError(t, err) {
		return
	}

	res, err := n.persistence.LoadEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, res.Events, 2) {
		return
	}
	assert.Equal(t, int64(2), res.StartSequence)
	assert.Equal(t, int64(2), res.Events[0].Sequence)
	assert.Equal(t, int64(3), res.Events[1].Sequence)
}

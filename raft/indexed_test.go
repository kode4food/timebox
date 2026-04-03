package raft_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestRestartIndexes(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	first := timebox.NewAggregateID("order", "1")
	second := timebox.NewAggregateID("order", "2")
	base := time.Unix(1_700_000_000, 0).UTC()

	assert.NoError(t, n.store.AppendEvents(first, 0, []*timebox.Event{
		indexedEvent(first, "active", "prod", base),
	}))
	assert.NoError(t, n.store.AppendEvents(second, 0, []*timebox.Event{
		indexedEvent(second, "paused", "stage", base.Add(time.Minute)),
	}))

	closeNode(t, n)

	n = newNode(t, cfg)
	waitForWrite(t, n.store)

	status, err := n.store.GetAggregateStatus(first)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "active", status)

	paused, err := n.store.ListAggregatesByStatus("paused")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        second,
		Timestamp: base.Add(time.Minute),
	}}, paused)

	stage, err := n.store.ListAggregatesByLabel("env", "stage")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{second}, stage)

	vals, err := n.store.ListLabelValues("env")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"prod", "stage"}, vals)
}

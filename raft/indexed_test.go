package raft_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestIndexes(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "indexes")
	base := time.Unix(1_700_000_000, 0).UTC()

	err := n.store.AppendEvents(id, 0, []*timebox.Event{
		indexedEvent(id, "active", "prod", base),
	})
	if !assert.NoError(t, err) {
		return
	}

	err = n.store.AppendEvents(id, 1, []*timebox.Event{
		indexedEvent(id, "paused", "stage", base.Add(time.Minute)),
	})
	if !assert.NoError(t, err) {
		return
	}

	status, err := n.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "paused", status)

	active, err := n.store.ListAggregatesByStatus("active")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, active)

	paused, err := n.store.ListAggregatesByStatus("paused")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        id,
		Timestamp: base.Add(time.Minute),
	}}, paused)

	prod, err := n.store.ListAggregatesByLabel("env", "prod")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, prod)

	stage, err := n.store.ListAggregatesByLabel("env", "stage")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{id}, stage)

	vals, err := n.store.ListLabelValues("env")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"stage"}, vals)

	err = n.store.AppendEvents(id, 2, []*timebox.Event{
		indexedEvent(id, "", "", base.Add(2*time.Minute)),
	})
	if !assert.NoError(t, err) {
		return
	}

	status, err = n.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, status)

	paused, err = n.store.ListAggregatesByStatus("paused")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, paused)

	stage, err = n.store.ListAggregatesByLabel("env", "stage")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, stage)

	vals, err = n.store.ListLabelValues("env")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, vals)
}

func TestQueries(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		name := "untrimmed"
		if trimEvents {
			name = "trimmed"
		}

		t.Run(name, func(t *testing.T) {
			n := newNode(t, nodeConfig{
				id:         "node-1",
				trimEvents: trimEvents,
			})
			waitForWrite(t, n.store)

			first := timebox.NewAggregateID("order", "1")
			second := timebox.NewAggregateID("order", "2")
			third := timebox.NewAggregateID("invoice", "1")
			base := time.Unix(1_700_000_000, 0).UTC()

			assert.NoError(t, n.store.AppendEvents(first, 0, []*timebox.Event{
				indexedEvent(first, "active", "prod", base),
			}))
			assert.NoError(t, n.store.AppendEvents(second, 0, []*timebox.Event{
				indexedEvent(second, "active", "prod", base.Add(time.Minute)),
			}))
			assert.NoError(t, n.store.AppendEvents(third, 0, []*timebox.Event{
				indexedEvent(third, "paused", "stage", base.Add(2*time.Minute)),
			}))

			ids, err := n.store.ListAggregates(timebox.NewAggregateID("order"))
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []timebox.AggregateID{first, second}, ids)

			status, err := n.store.GetAggregateStatus(first)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, "active", status)

			status, err = n.store.GetAggregateStatus(
				timebox.NewAggregateID("missing", "1"),
			)
			if !assert.NoError(t, err) {
				return
			}
			assert.Empty(t, status)

			active, err := n.store.ListAggregatesByStatus("active")
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []timebox.StatusEntry{
				{ID: first, Timestamp: base},
				{ID: second, Timestamp: base.Add(time.Minute)},
			}, active)

			missing, err := n.store.ListAggregatesByStatus("missing")
			if !assert.NoError(t, err) {
				return
			}
			assert.Empty(t, missing)

			prod, err := n.store.ListAggregatesByLabel("env", "prod")
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []timebox.AggregateID{first, second}, prod)

			vals, err := n.store.ListLabelValues("env")
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []string{"prod", "stage"}, vals)
		})
	}
}

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

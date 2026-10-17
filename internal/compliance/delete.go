package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runDelete(t *testing.T, p Profile) {
	t.Run("Lifecycle", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{
			Indexer: newIndexer(t),
		}, nil)
		id := timebox.NewAggregateID("order", "delete")
		active := "active"
		ev := testEvent(t,
			time.Unix(1_700_000_030, 0).UTC(),
			"event.test", 1, &active,
			map[string]bool{"prod": true},
		)
		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 1}, 1))

		assert.NoError(t, backend.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 1,
			Events:           []*timebox.Event{ev},
			Delete:           true,
		}))

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Empty(t, evs)

		ids, err := store.ListAggregates("order")
		assert.NoError(t, err)
		assert.Empty(t, ids)

		statuses, err := store.ListAggregatesByStatus(timebox.StatusQuery{
			Status: active,
		})
		assert.NoError(t, err)
		assert.Empty(t, statuses)

		ids, err = store.ListAggregatesByTag("prod")
		assert.NoError(t, err)
		assert.Empty(t, ids)

		snap, err := store.GetSnapshot[map[string]int](id)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), snap.NextSequence)
		assert.Empty(t, snap.State)

		archived, err := backend.LoadEvents(timebox.LoadEventsRequest{
			ID: timebox.ArchiveQueueID,
		})
		assert.NoError(t, err)
		assert.Empty(t, archived.Events)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		evs, err = store.GetEvents(id, 0)
		assert.NoError(t, err)
		if assert.Len(t, evs, 1) {
			assert.Equal(t, int64(0), evs[0].Sequence)
		}
	})

	t.Run("Conflict", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{}, nil)
		id := timebox.NewAggregateID("order", "delete-conflict")
		ev := testEvent(t,
			time.Unix(1_700_000_031, 0).UTC(), "event.test", 1, nil, nil,
		)
		assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{ev, ev}),
		)

		err := backend.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 1,
			Delete:           true,
		})
		var conflict *timebox.VersionConflictError
		if assert.ErrorAs(t, err, &conflict) {
			assert.Equal(t, int64(2), conflict.ActualSequence)
		}

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Len(t, evs, 2)
	})

	t.Run("Mutations", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{}, nil)
		id := timebox.NewAggregateID("order", "delete-mutations")
		at := time.Unix(1_700_000_033, 0).UTC()
		ev := testEvent(t, at, "event.test", 1, nil, nil)
		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

		active := "active"
		assert.NoError(t, backend.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 1,
			Events:           []*timebox.Event{ev},
			Status:           &active,
			StatusAt:         at,
			Tags:             map[string]bool{"prod": true},
			Delete:           true,
		}))

		statuses, err := store.ListAggregatesByStatus(timebox.StatusQuery{
			Status: active,
		})
		assert.NoError(t, err)
		assert.Empty(t, statuses)

		ids, err := store.ListAggregatesByTag("prod")
		assert.NoError(t, err)
		assert.Empty(t, ids)

		ids, err = store.ListAggregates("order")
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})

	t.Run("Atomic", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{}, nil)
		deleted := timebox.NewAggregateID("order", "delete-atomic")
		stale := timebox.NewAggregateID("order", "delete-stale")
		ev := testEvent(t,
			time.Unix(1_700_000_032, 0).UTC(), "event.test", 1, nil, nil,
		)
		assert.NoError(t, store.AppendEvents(deleted, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.AppendEvents(stale, 0, []*timebox.Event{ev}))

		err := backend.Append(
			timebox.AppendRequest{
				ID:               deleted,
				ExpectedSequence: 1,
				Delete:           true,
			},
			timebox.AppendRequest{
				ID:               stale,
				ExpectedSequence: 0,
				Events:           []*timebox.Event{ev},
			},
		)
		var conflict *timebox.VersionConflictError
		assert.ErrorAs(t, err, &conflict)

		evs, err := store.GetEvents(deleted, 0)
		assert.NoError(t, err)
		assert.Len(t, evs, 1)
	})
}

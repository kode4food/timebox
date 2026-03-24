package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runSnapshots(t *testing.T, p Profile) {
	t.Run("Missing", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "missing")

		var state map[string]int
		snap, err := store.GetSnapshot(id, &state)
		assert.NoError(t, err)
		assert.Nil(t, state)
		assert.NotNil(t, snap)
		assert.Equal(t, int64(0), snap.NextSequence)
		assert.Empty(t, snap.AdditionalEvents)
		assert.False(t, snap.ShouldSnapshot)
	})

	t.Run("BeforeEvents", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "before-events")

		assert.NoError(t, store.PutSnapshot(id, map[string]int{"v": 0}, 0))

		var state map[string]int
		snap, err := store.GetSnapshot(id, &state)
		assert.NoError(t, err)
		assert.Equal(t, map[string]int{"v": 0}, state)
		assert.Equal(t, int64(0), snap.NextSequence)
		assert.Empty(t, snap.AdditionalEvents)
		assert.False(t, snap.ShouldSnapshot)

		ids, err := store.ListAggregates(timebox.NewAggregateID("order"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)
	})

	t.Run("RoundTrip", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "snapshot")
		ev := testEvent(t,
			time.Unix(1_700_000_012, 0).UTC(),
			"event.first", 1, nil, nil,
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

		var empty map[string]int
		snap, err := store.GetSnapshot(id, &empty)
		assert.NoError(t, err)
		assert.Nil(t, empty)
		assert.Equal(t, int64(0), snap.NextSequence)
		assert.True(t, snap.ShouldSnapshot)
		if !assert.Len(t, snap.AdditionalEvents, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(0), snap.AdditionalEvents[0].Sequence)

		assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 1}, 1))
		assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 0}, 0))
		assert.NoError(t,
			store.AppendEvents(id, 1, []*timebox.Event{
				testEvent(t,
					time.Unix(1_700_000_013, 0).UTC(),
					"event.second", 2, nil, nil,
				),
			}),
		)

		var state map[string]int
		snap, err = store.GetSnapshot(id, &state)
		assert.NoError(t, err)
		assert.Equal(t, map[string]int{"value": 1}, state)
		assert.Equal(t, int64(1), snap.NextSequence)
		if !assert.Len(t, snap.AdditionalEvents, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(1), snap.AdditionalEvents[0].Sequence)
		assert.Equal(t,
			timebox.EventType("event.second"),
			snap.AdditionalEvents[0].Type,
		)
	})

	t.Run("Trim", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{TrimEvents: true})
		id := timebox.NewAggregateID("order", "trim")

		assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{
				testEvent(t,
					time.Unix(1_700_000_014, 0).UTC(),
					"event.one", 1, nil, nil,
				),
				testEvent(t,
					time.Unix(1_700_000_015, 0).UTC(),
					"event.two", 2, nil, nil,
				),
				testEvent(t,
					time.Unix(1_700_000_016, 0).UTC(),
					"event.three", 3, nil, nil,
				),
			}),
		)
		assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 3}, 3))

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Empty(t, evs)

		assert.NoError(t,
			store.AppendEvents(id, 3, []*timebox.Event{
				testEvent(t,
					time.Unix(1_700_000_017, 0).UTC(),
					"event.four", 4, nil, nil,
				),
				testEvent(t,
					time.Unix(1_700_000_018, 0).UTC(),
					"event.five", 5, nil, nil,
				),
			}),
		)

		evs, err = store.GetEvents(id, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 2) {
			t.FailNow()
		}
		assert.Equal(t, int64(3), evs[0].Sequence)
		assert.Equal(t, int64(4), evs[1].Sequence)

		var state map[string]int
		snap, err := store.GetSnapshot(id, &state)
		assert.NoError(t, err)
		assert.Equal(t, map[string]int{"value": 3}, state)
		assert.Equal(t, int64(3), snap.NextSequence)
		if !assert.Len(t, snap.AdditionalEvents, 2) {
			t.FailNow()
		}
		assert.Equal(t, int64(3), snap.AdditionalEvents[0].Sequence)
		assert.Equal(t, int64(4), snap.AdditionalEvents[1].Sequence)
	})

	t.Run("NoTrim", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "no-trim")

		assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{
				testEvent(t,
					time.Unix(1_700_000_019, 0).UTC(),
					"event.one", 1, nil, nil,
				),
				testEvent(t,
					time.Unix(1_700_000_020, 0).UTC(),
					"event.two", 2, nil, nil,
				),
			}),
		)
		assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 1}, 1))

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Len(t, evs, 2)
	})

	t.Run("MarshalError", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		err := store.PutSnapshot(
			timebox.NewAggregateID("order", "bad-snapshot"),
			func() {}, 1,
		)
		assert.Error(t, err)
	})
}

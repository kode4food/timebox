package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runEvents(t *testing.T, p Profile) {
	t.Run("Missing", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "missing")

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Empty(t, evs)
	})

	t.Run("EmptyAppend", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "empty")

		err := store.AppendEvents(id, 0, nil)
		assert.NoError(t, err)

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Empty(t, evs)
	})

	t.Run("AppendLoad", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "append")
		first := testEvent(t,
			time.Unix(1_700_000_000, 123).UTC(),
			"event.first", 1, nil, nil,
		)
		second := testEvent(t,
			time.Unix(1_700_000_001, 456).UTC(),
			"event.second", 2, nil, nil,
		)

		err := store.AppendEvents(id, 0, []*timebox.Event{first, second})
		assert.NoError(t, err)

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 2) {
			t.FailNow()
		}

		assert.Equal(t, first.Timestamp, evs[0].Timestamp)
		assert.Equal(t, int64(0), evs[0].Sequence)
		assert.Equal(t, first.Type, evs[0].Type)
		assert.Equal(t, id, evs[0].AggregateID)
		assert.Equal(t, first.Data, evs[0].Data)

		assert.Equal(t, second.Timestamp, evs[1].Timestamp)
		assert.Equal(t, int64(1), evs[1].Sequence)
		assert.Equal(t, second.Type, evs[1].Type)
		assert.Equal(t, id, evs[1].AggregateID)
		assert.Equal(t, second.Data, evs[1].Data)

		evs, err = store.GetEvents(id, 1)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(1), evs[0].Sequence)
		assert.Equal(t, second.Type, evs[0].Type)

		evs, err = store.GetEvents(id, 2)
		assert.NoError(t, err)
		assert.Empty(t, evs)
	})

	t.Run("AppendCopy", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "copy")
		other := timebox.NewAggregateID("other", "copy")
		first := testEvent(t,
			time.Unix(1_700_000_002, 0).UTC(),
			"event.first", 1, nil, nil,
		)
		second := testEvent(t,
			time.Unix(1_700_000_003, 0).UTC(),
			"event.second", 2, nil, nil,
		)
		first.Sequence = 41
		first.AggregateID = other
		second.Sequence = 42
		second.AggregateID = other

		err := store.AppendEvents(id, 0, []*timebox.Event{first, second})
		assert.NoError(t, err)

		assert.Equal(t, int64(41), first.Sequence)
		assert.Equal(t, other, first.AggregateID)
		assert.Equal(t, int64(42), second.Sequence)
		assert.Equal(t, other, second.AggregateID)

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 2) {
			t.FailNow()
		}
		assert.Equal(t, int64(0), evs[0].Sequence)
		assert.Equal(t, int64(1), evs[1].Sequence)
		assert.Equal(t, id, evs[0].AggregateID)
		assert.Equal(t, id, evs[1].AggregateID)
	})

	t.Run("Value", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "value")
		ev := testEvent(t,
			time.Unix(1_700_000_022, 0).UTC(),
			"event.value", 7, nil, map[string]string{"env": "dev"},
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}

		got, err := timebox.GetEventValue[indexData](evs[0])
		assert.NoError(t, err)
		assert.Equal(t, indexData{
			Value:  7,
			Labels: map[string]string{"env": "dev"},
		}, got)
	})

	t.Run("Conflict", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "conflict")

		err := store.AppendEvents(id, 0, []*timebox.Event{
			testEvent(t,
				time.Unix(1_700_000_004, 0).UTC(),
				"event.first", 1, nil, nil,
			),
			testEvent(t,
				time.Unix(1_700_000_005, 0).UTC(),
				"event.second", 2, nil, nil,
			),
		})
		assert.NoError(t, err)

		err = store.AppendEvents(id, 1, []*timebox.Event{
			testEvent(t,
				time.Unix(1_700_000_006, 0).UTC(),
				"event.stale", 3, nil, nil,
			),
		})
		assert.Error(t, err)

		var conflict *timebox.VersionConflictError
		if !assert.ErrorAs(t, err, &conflict) {
			t.FailNow()
		}
		assert.Equal(t, int64(1), conflict.ExpectedSequence)
		assert.Equal(t, int64(2), conflict.ActualSequence)
		if !assert.Len(t, conflict.NewEvents, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(1), conflict.NewEvents[0].Sequence)
		assert.Equal(t,
			timebox.EventType("event.second"),
			conflict.NewEvents[0].Type,
		)
	})

	t.Run("EmptyAppendConflict", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "empty-conflict")

		err := store.AppendEvents(id, 0, []*timebox.Event{
			testEvent(t,
				time.Unix(1_700_000_007, 0).UTC(),
				"event.first", 1, nil, nil,
			),
		})
		assert.NoError(t, err)

		err = store.AppendEvents(id, 0, nil)
		assert.Error(t, err)

		var conflict *timebox.VersionConflictError
		if !assert.ErrorAs(t, err, &conflict) {
			t.FailNow()
		}
		assert.Equal(t, int64(0), conflict.ExpectedSequence)
		assert.Equal(t, int64(1), conflict.ActualSequence)
		if !assert.Len(t, conflict.NewEvents, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(0), conflict.NewEvents[0].Sequence)
		assert.Equal(t,
			timebox.EventType("event.first"),
			conflict.NewEvents[0].Type,
		)
	})
}

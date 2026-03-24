package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runAggregates(t *testing.T, p Profile) {
	t.Run("Empty", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})

		all, err := store.ListAggregates(nil)
		assert.NoError(t, err)
		assert.Empty(t, all)
	})

	t.Run("List", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		first := timebox.NewAggregateID("order", "1")
		second := timebox.NewAggregateID("order", "2")
		third := timebox.NewAggregateID("user", "1")
		ev := testEvent(t,
			time.Unix(1_700_000_007, 0).UTC(),
			"event.test", 1, nil, nil,
		)

		assert.NoError(t, store.AppendEvents(first, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.AppendEvents(second, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.AppendEvents(third, 0, []*timebox.Event{ev}))

		all, err := store.ListAggregates(nil)
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]timebox.AggregateID{first, second, third},
			all,
		)

		orders, err := store.ListAggregates(timebox.NewAggregateID("order"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{first, second}, orders)

		users, err := store.ListAggregates(timebox.NewAggregateID("user"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{third}, users)
	})

	t.Run("AggregateIDChars", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{
			Indexer: newIndexer(t),
		})
		id := timebox.NewAggregateID("order-1", `part;2`, `quoted["3"]`)
		active := "active"
		ev := testEvent(t,
			time.Unix(1_700_000_008, 0).UTC(),
			"event.test", 1, &active,
			map[string]string{"env": "dev"},
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}
		assert.Equal(t, id, evs[0].AggregateID)

		ids, err := store.ListAggregates(timebox.NewAggregateID("order-1"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)

		ids, err = store.ListAggregatesByLabel("env", "dev")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)
	})
}

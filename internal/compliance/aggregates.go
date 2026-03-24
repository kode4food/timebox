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
		id := timebox.NewAggregateID(`order:1`, `part\2`, `quoted["3"]`)
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

		ids, err := store.ListAggregates(timebox.NewAggregateID(`order:1`))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)

		ids, err = store.ListAggregatesByLabel("env", "dev")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)
	})

	t.Run("AggregateIDEscaping", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		redisEsc := timebox.NewAggregateID("order:1", "item")
		redisRaw := timebox.NewAggregateID("order", "1", "item")
		memEsc := timebox.NewAggregateID("mem\x1f1", "item")
		memRaw := timebox.NewAggregateID("mem", "1\x1fitem")
		at := time.Unix(1_700_000_009, 0).UTC()

		assert.NoError(t, store.AppendEvents(redisEsc, 0, []*timebox.Event{
			testEvent(t, at, "event.redis-esc", 1, nil, nil),
		}))
		assert.NoError(t, store.AppendEvents(redisRaw, 0, []*timebox.Event{
			testEvent(t, at, "event.redis-raw", 2, nil, nil),
		}))
		assert.NoError(t, store.AppendEvents(memEsc, 0, []*timebox.Event{
			testEvent(t, at, "event.mem-esc", 3, nil, nil),
		}))
		assert.NoError(t, store.AppendEvents(memRaw, 0, []*timebox.Event{
			testEvent(t, at, "event.mem-raw", 4, nil, nil),
		}))

		evs, err := store.GetEvents(redisEsc, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}
		assert.Equal(t, redisEsc, evs[0].AggregateID)

		evs, err = store.GetEvents(redisRaw, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}
		assert.Equal(t, redisRaw, evs[0].AggregateID)

		evs, err = store.GetEvents(memEsc, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}
		assert.Equal(t, memEsc, evs[0].AggregateID)

		evs, err = store.GetEvents(memRaw, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}
		assert.Equal(t, memRaw, evs[0].AggregateID)

		ids, err := store.ListAggregates(timebox.NewAggregateID("order:1"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{redisEsc}, ids)

		ids, err = store.ListAggregates(timebox.NewAggregateID("order"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{redisRaw}, ids)

		ids, err = store.ListAggregates(timebox.NewAggregateID("mem\x1f1"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{memEsc}, ids)

		ids, err = store.ListAggregates(timebox.NewAggregateID("mem"))
		assert.NoError(t, err)
		assert.ElementsMatch(t, []timebox.AggregateID{memRaw}, ids)
	})
}

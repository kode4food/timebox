package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runAggregates(t *testing.T, p Profile) {
	t.Run("AggregateIDChars", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{
			Indexer: newIndexer(t),
		})
		id := timebox.NewAggregateID(`order:1`, `part\2 quoted["3"]`)
		ev := testEvent(t,
			time.Unix(1_700_000_008, 0).UTC(),
			"event.test", 1, new("active"),
			map[string]bool{"dev": true},
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		if !assert.Len(t, evs, 1) {
			t.FailNow()
		}
		assert.Equal(t, id, evs[0].AggregateID)

		entries := listStatus(t, store, "active", `order:1`)
		if assert.Len(t, entries, 1) {
			assert.Equal(t, id, entries[0].ID)
		}
		assert.Equal(t, []string{"dev"}, tagsOf(t, store, id))
	})

	t.Run("AggregateIDEscaping", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		redisEsc := timebox.NewAggregateID("order:1", "item")
		redisRaw := timebox.NewAggregateID("order", "1:item")
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
	})
}

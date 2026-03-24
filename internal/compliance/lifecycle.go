package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runLifecycle(t *testing.T, p Profile) {
	t.Run("Ready", func(t *testing.T) {
		_ = openStore(t, p, StoreConfig{})
	})

	t.Run("Closed", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{
			Indexer: newIndexer(t),
		})
		id := timebox.NewAggregateID("order", "closed")
		ev := testEvent(t,
			time.Unix(1_700_000_021, 0).UTC(),
			"event.test", 1, nil, nil,
		)

		assert.NoError(t, store.Close())

		err := store.AppendEvents(id, 0, []*timebox.Event{ev})
		assert.Error(t, err)

		_, err = store.GetEvents(id, 0)
		assert.Error(t, err)

		var state map[string]int
		_, err = store.GetSnapshot(id, &state)
		assert.Error(t, err)

		err = store.PutSnapshot(id, map[string]int{"value": 1}, 1)
		assert.Error(t, err)

		_, err = store.ListAggregates(nil)
		assert.Error(t, err)

		_, err = store.ListAggregatesByStatus("active")
		assert.Error(t, err)

		_, err = store.ListAggregatesByLabel("env", "prod")
		assert.Error(t, err)

		_, err = store.ListLabelValues("env")
		assert.Error(t, err)
	})
}

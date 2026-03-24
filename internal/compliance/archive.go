package compliance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runArchive(t *testing.T, p Profile) {
	if !p.Archive {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "archive-disabled")

		err := store.Archive(id)
		assert.ErrorIs(t, err, timebox.ErrArchivingDisabled)

		err = store.ConsumeArchive(context.Background(), nil)
		assert.ErrorIs(t, err, timebox.ErrArchivingDisabled)
		return
	}

	t.Run("Lifecycle", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{
			Indexer: newIndexer(t),
		})
		id := timebox.NewAggregateID("order", "archive")
		active := "active"
		ev := testEvent(t,
			time.Unix(1_700_000_019, 0).UTC(),
			"event.test", 1, &active,
			map[string]string{"env": "prod"},
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 1}, 1))
		assert.NoError(t, store.Archive(id))

		evs, err := store.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Empty(t, evs)

		ids, err := store.ListAggregates(timebox.NewAggregateID("order"))
		assert.NoError(t, err)
		assert.Empty(t, ids)

		statuses, err := store.ListAggregatesByStatus(active)
		assert.NoError(t, err)
		assert.Empty(t, statuses)

		ids, err = store.ListAggregatesByLabel("env", "prod")
		assert.NoError(t, err)
		assert.Empty(t, ids)

		vals, err := store.ListLabelValues("env")
		assert.NoError(t, err)
		assert.Empty(t, vals)

		var rec *timebox.ArchiveRecord
		err = store.ConsumeArchive(context.Background(), func(
			_ context.Context, item *timebox.ArchiveRecord,
		) error {
			rec = item
			return nil
		})
		assert.NoError(t, err)
		if !assert.NotNil(t, rec) {
			t.FailNow()
		}
		assert.Equal(t, id, rec.AggregateID)
		assert.Equal(t, int64(1), rec.SnapshotSequence)
		assert.Equal(t, json.RawMessage(`{"value":1}`), rec.SnapshotData)
		if !assert.Len(t, rec.Events, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(0), rec.Events[0].Sequence)
		assert.Equal(t, ev.Type, rec.Events[0].Type)
	})

	t.Run("Missing", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		err := store.Archive(timebox.NewAggregateID("order", "missing"))
		assert.NoError(t, err)
	})

	t.Run("NoSnapshot", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "archive-no-snapshot")
		ev := testEvent(t,
			time.Unix(1_700_000_021, 0).UTC(),
			"event.test", 1, nil, nil,
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.Archive(id))

		var rec *timebox.ArchiveRecord
		err := store.ConsumeArchive(context.Background(), func(
			_ context.Context, item *timebox.ArchiveRecord,
		) error {
			rec = item
			return nil
		})
		assert.NoError(t, err)
		if !assert.NotNil(t, rec) {
			t.FailNow()
		}
		assert.Equal(t, id, rec.AggregateID)
		assert.Empty(t, rec.SnapshotData)
		assert.Equal(t, int64(0), rec.SnapshotSequence)
		if !assert.Len(t, rec.Events, 1) {
			t.FailNow()
		}
		assert.Equal(t, int64(0), rec.Events[0].Sequence)
	})

	t.Run("NoHandler", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		err := store.ConsumeArchive(context.Background(), nil)
		assert.ErrorIs(t, err, timebox.ErrArchiveHandlerMissing)
	})

	t.Run("NoMessages", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()

		called := false
		err := store.ConsumeArchive(ctx, func(
			_ context.Context, _ *timebox.ArchiveRecord,
		) error {
			called = true
			return nil
		})
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, called)
	})

	t.Run("HandlerErrorKeepsRecord", func(t *testing.T) {
		store := openStore(t, p, StoreConfig{})
		id := timebox.NewAggregateID("order", "handler")
		ev := testEvent(t,
			time.Unix(1_700_000_020, 0).UTC(),
			"event.test", 1, nil, nil,
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.Archive(id))

		handlerErr := errors.New("handler failed")
		err := store.ConsumeArchive(context.Background(), func(
			_ context.Context, rec *timebox.ArchiveRecord,
		) error {
			assert.Equal(t, id, rec.AggregateID)
			return handlerErr
		})
		assert.ErrorIs(t, err, handlerErr)

		var rec *timebox.ArchiveRecord
		err = store.ConsumeArchive(context.Background(), func(
			_ context.Context, item *timebox.ArchiveRecord,
		) error {
			rec = item
			return nil
		})
		assert.NoError(t, err)
		if !assert.NotNil(t, rec) {
			t.FailNow()
		}
		assert.Equal(t, id, rec.AggregateID)
	})
}

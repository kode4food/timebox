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
	t.Run("QueueTrims", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{}, nil)
		first := timebox.NewAggregateID("order", "trim-first")
		second := timebox.NewAggregateID("order", "trim-second")
		for _, id := range []timebox.AggregateID{first, second} {
			ev := testEvent(t, time.Now().UTC(), "event.test", 1, nil, nil)
			assert.NoError(t,
				store.AppendEvents(id, 0, []*timebox.Event{ev}),
			)
			assert.NoError(t, store.Archive(id))
		}

		assert.Equal(t, first, consumeArchiveID(t, store))

		evs, err := backend.LoadEvents(timebox.LoadEventsRequest{
			ID: timebox.ArchiveQueueID,
		})
		assert.NoError(t, err)
		if assert.Len(t, evs.Events, 1) {
			assert.Equal(t, int64(2), evs.StartSequence)
			assert.Equal(t, int64(2), evs.Events[0].Sequence)
		}

		third := timebox.NewAggregateID("order", "trim-third")
		ev := testEvent(t, time.Now().UTC(), "event.test", 1, nil, nil)
		assert.NoError(t,
			store.AppendEvents(third, 0, []*timebox.Event{ev}),
		)
		assert.NoError(t, store.Archive(third))

		reloaded, err := timebox.NewStore(backend)
		assert.NoError(t, err)
		assert.Equal(t, second, consumeArchiveID(t, reloaded))
		assert.Equal(t, third, consumeArchiveID(t, reloaded))
	})

	t.Run("Lifecycle", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{
			Indexer: newIndexer(t),
		})
		id := timebox.NewAggregateID("order", "archive")
		active := "active"
		ev := testEvent(t,
			time.Unix(1_700_000_019, 0).UTC(),
			"event.test", 1, &active,
			map[string]bool{"prod": true},
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.PutSnapshot(id, map[string]int{"value": 1}, 1))
		assert.NoError(t, store.Archive(id))

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

		var rec *timebox.ArchiveRecord
		err = store.ConsumeArchive(context.Background(),
			func(_ context.Context, item *timebox.ArchiveRecord) error {
				rec = item
				return nil
			},
		)
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
		store := openStore(t, p, timebox.Config{})
		err := store.Archive(timebox.NewAggregateID("order", "missing"))
		assert.NoError(t, err)
	})

	t.Run("NoSnapshot", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		id := timebox.NewAggregateID("order", "archive-no-snapshot")
		ev := testEvent(t,
			time.Unix(1_700_000_021, 0).UTC(),
			"event.test", 1, nil, nil,
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.Archive(id))

		var rec *timebox.ArchiveRecord
		err := store.ConsumeArchive(context.Background(),
			func(_ context.Context, item *timebox.ArchiveRecord) error {
				rec = item
				return nil
			},
		)
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
		store := openStore(t, p, timebox.Config{})
		err := store.ConsumeArchive(context.Background(), nil)
		assert.ErrorIs(t, err, timebox.ErrArchiveHandlerMissing)
	})

	t.Run("NoMessages", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		defer cancel()

		called := false
		err := store.ConsumeArchive(ctx,
			func(_ context.Context, _ *timebox.ArchiveRecord) error {
				called = true
				return nil
			},
		)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, called)
	})

	t.Run("HandlerErrorKeepsRecord", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		id := timebox.NewAggregateID("order", "handler")
		ev := testEvent(t,
			time.Unix(1_700_000_020, 0).UTC(),
			"event.test", 1, nil, nil,
		)

		assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
		assert.NoError(t, store.Archive(id))

		handlerErr := errors.New("handler failed")
		err := store.ConsumeArchive(context.Background(),
			func(_ context.Context, rec *timebox.ArchiveRecord) error {
				assert.Equal(t, id, rec.AggregateID)
				return handlerErr
			},
		)
		assert.ErrorIs(t, err, handlerErr)

		var rec *timebox.ArchiveRecord
		err = store.ConsumeArchive(context.Background(),
			func(_ context.Context, item *timebox.ArchiveRecord) error {
				rec = item
				return nil
			},
		)
		assert.NoError(t, err)
		if !assert.NotNil(t, rec) {
			t.FailNow()
		}
		assert.Equal(t, id, rec.AggregateID)
	})
}

func consumeArchiveID(
	t *testing.T, store *timebox.Store,
) timebox.AggregateID {
	t.Helper()
	var res timebox.AggregateID
	err := store.ConsumeArchive(t.Context(),
		func(_ context.Context, rec *timebox.ArchiveRecord) error {
			res = rec.AggregateID
			return nil
		},
	)
	assert.NoError(t, err)
	return res
}

package timebox_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
)

func TestArchiveToStream(t *testing.T) {
	store, err := memory.NewStore(timebox.Config{
		TrimEvents: true,
		Indexer: func([]*timebox.Event) []*timebox.Index {
			return []*timebox.Index{{
				Status: new("active"),
				Tags:   map[string]bool{"prod": true},
			}}
		},
	})
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")

	err = store.PutSnapshot(id, map[string]int{"value": 3}, 2)
	assert.NoError(t, err)

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t,
		store.AppendEvents(id, 2, []*timebox.Event{ev}),
	)

	err = store.Archive(id)
	assert.NoError(t, err)

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 0)

	aggregates, err := store.ListAggregates(id.Type)
	assert.NoError(t, err)
	assert.Empty(t, aggregates)
	statuses, err := store.ListAggregatesByStatus("active")
	assert.NoError(t, err)
	assert.Empty(t, statuses)
	tagIDs, err := store.ListAggregatesByTag("prod")
	assert.NoError(t, err)
	assert.Empty(t, tagIDs)
	var handled *timebox.ArchiveRecord
	err = store.ConsumeArchive(ctx,
		func(_ context.Context, rec *timebox.ArchiveRecord) error {
			handled = rec
			return nil
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, handled)
	assert.Equal(t, id, handled.AggregateID)
	assert.Len(t, handled.Events, 1)
}

func TestConsumeArchive(t *testing.T) {
	store, err := memory.NewStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "consume")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t,
		store.AppendEvents(id, 0, []*timebox.Event{ev}),
	)
	assert.NoError(t, store.Archive(id))

	var handled *timebox.ArchiveRecord
	err = store.ConsumeArchive(ctx,
		func(_ context.Context, record *timebox.ArchiveRecord) error {
			handled = record
			return nil
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, handled)
	assert.Equal(t, id, handled.AggregateID)
	assert.Len(t, handled.Events, 1)

}

func TestConsumeArchiveNoHandler(t *testing.T) {
	store, err := memory.NewStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.ConsumeArchive(context.Background(), nil)
	assert.ErrorIs(t, err, timebox.ErrArchiveHandlerMissing)
}

func TestConsumeArchiveNoMessages(t *testing.T) {
	store, err := memory.NewStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	called := false
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	err = store.ConsumeArchive(ctx,
		func(_ context.Context, _ *timebox.ArchiveRecord) error {
			called = true
			return nil
		},
	)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, called)
}

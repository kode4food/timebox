package memory_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
)

func TestNewStore(t *testing.T) {
	t.Run("Config", func(t *testing.T) {
		s, err := memory.Open().NewStore(timebox.Config{MaxRetries: 3})
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, 3, s.Config().MaxRetries)
	})

	t.Run("Invalid", func(t *testing.T) {
		s, err := memory.Open().NewStore(timebox.Config{MaxRetries: -1})
		assert.ErrorIs(t, err, timebox.ErrInvalidMaxRetries)
		assert.Nil(t, s)
	})
}

func TestClosedMethods(t *testing.T) {
	b := memory.Open()
	assert.NoError(t, b.Close())

	_, err := b.LoadEvents(timebox.LoadEventsRequest{
		ID:      timebox.NewAggregateID("order", "1"),
		FromSeq: 0,
	})
	assert.ErrorIs(t, err, memory.ErrClosed)

	err = b.ConsumeArchive(context.Background(),
		func(_ context.Context, _ *timebox.ArchiveRecord) error {
			return nil
		},
	)
	assert.ErrorIs(t, err, memory.ErrClosed)

	err = b.Append(timebox.AppendRequest{
		ID:               timebox.NewAggregateID("order", "1"),
		ExpectedSequence: 0,
		Events:           testEvents("created"),
	})
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = b.GetAggregateStatus(timebox.NewAggregateID("order", "1"))
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = b.ListAggregatesByStatus("active")
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = b.ListAggregatesByTag("prod")
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = b.ListAggregates("")
	assert.ErrorIs(t, err, memory.ErrClosed)
}

func testEvents(types ...timebox.EventType) []*timebox.Event {
	res := make([]*timebox.Event, len(types))
	for i, typ := range types {
		res[i] = &timebox.Event{
			Timestamp: time.Unix(int64(i), 0).UTC(),
			Type:      typ,
			Data:      json.RawMessage(`{}`),
		}
	}
	return res
}

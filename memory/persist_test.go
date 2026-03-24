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

func TestClosedMethods(t *testing.T) {
	p := memory.NewPersistence(timebox.Config{})
	assert.NoError(t, p.Close())

	_, err := p.LoadEvents(timebox.NewAggregateID("order", "1"), 0)
	assert.ErrorIs(t, err, memory.ErrClosed)

	err = p.ConsumeArchive(context.Background(), func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = p.Append(timebox.AppendRequest{
		ID:               timebox.NewAggregateID("order", "1"),
		ExpectedSequence: 0,
		Events:           testEvents("created"),
	})
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = p.GetAggregateStatus(timebox.NewAggregateID("order", "1"))
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = p.ListAggregatesByStatus("active")
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = p.ListAggregatesByLabel("env", "prod")
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = p.ListLabelValues("env")
	assert.ErrorIs(t, err, memory.ErrClosed)

	_, err = p.ListAggregates(nil)
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

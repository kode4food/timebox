package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runPublisher(t *testing.T, p Profile) {
	t.Helper()
	published := make(chan []*timebox.Event, 2)
	_, store := openBackend(t, p, timebox.Config{},
		func(evs ...*timebox.Event) {
			published <- evs
		},
	)
	id := timebox.NewAggregateID("order", "publisher")
	ev := testEvent(t, time.Now(), "created", 1, nil, nil)
	assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))
	select {
	case evs := <-published:
		if assert.Len(t, evs, 1) {
			assert.Equal(t, id, evs[0].AggregateID)
			assert.Equal(t, ev.Type, evs[0].Type)
		}
	case <-time.After(readyTimeout):
		t.Fail()
	}

	err := store.AppendEvents(id, 0, []*timebox.Event{ev})
	assert.Error(t, err)
	select {
	case <-published:
		t.Fail()
	case <-time.After(100 * time.Millisecond):
	}
}

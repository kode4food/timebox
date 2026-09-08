package compliance

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

type (
	Profile struct {
		Open    Opener
		Archive bool
	}

	// Opener opens a backend and a Store bound to it
	Opener func(*testing.T, timebox.Config) (timebox.Backend, *timebox.Store)

	indexData struct {
		Value  int             `json:"value"`
		Status *string         `json:"status,omitempty"`
		Tags   map[string]bool `json:"tags,omitempty"`
	}
)

const readyTimeout = 15 * time.Second

func openStore(t *testing.T, p Profile, cfg timebox.Config) *timebox.Store {
	t.Helper()

	_, store := openBackend(t, p, cfg)
	return store
}

func openBackend(
	t *testing.T, p Profile, cfg timebox.Config,
) (timebox.Backend, *timebox.Store) {
	t.Helper()

	backend, store := p.Open(t, cfg)
	if !assert.NotNil(t, store) {
		t.FailNow()
	}

	ctx, cancel := context.WithTimeout(t.Context(), readyTimeout)
	defer cancel()

	if !assert.NoError(t, store.WaitReady(ctx)) {
		t.FailNow()
	}
	return backend, store
}

func testEvent(
	t *testing.T, at time.Time, typ timebox.EventType, value int,
	status *string, tags map[string]bool,
) *timebox.Event {
	t.Helper()

	data, err := json.Marshal(indexData{
		Value:  value,
		Status: status,
		Tags:   tags,
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	return &timebox.Event{
		Timestamp: at,
		Type:      typ,
		Data:      data,
	}
}

func newIndexer(t *testing.T) timebox.Indexer {
	t.Helper()

	return func(evs []*timebox.Event) []*timebox.Index {
		t.Helper()

		res := make([]*timebox.Index, 0, len(evs))
		for _, ev := range evs {
			var data indexData
			if !assert.NoError(t, json.Unmarshal(ev.Data, &data)) {
				return nil
			}
			res = append(res, &timebox.Index{
				Status: data.Status,
				Tags:   data.Tags,
			})
		}
		return res
	}
}

func encodedSize(t *testing.T, value any) int {
	t.Helper()

	data, err := json.Marshal(value)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return len(data)
}

func eventsDataSize(evs []*timebox.Event) int {
	size := 0
	for _, ev := range evs {
		size += len(ev.Data)
	}
	return size
}

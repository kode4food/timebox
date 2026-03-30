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
	StoreConfig struct {
		Indexer    timebox.Indexer
		TrimEvents bool
	}

	Profile struct {
		Archive  bool
		NewStore func(*testing.T, StoreConfig) *timebox.Store
	}

	indexData struct {
		Value  int               `json:"value"`
		Status *string           `json:"status,omitempty"`
		Labels map[string]string `json:"labels,omitempty"`
	}
)

const readyTimeout = 15 * time.Second

func openStore(t *testing.T, p Profile, cfg StoreConfig) *timebox.Store {
	t.Helper()

	store := p.NewStore(t, cfg)
	if !assert.NotNil(t, store) {
		t.FailNow()
	}

	ctx, cancel := context.WithTimeout(t.Context(), readyTimeout)
	defer cancel()

	if !assert.NoError(t, store.WaitReady(ctx)) {
		t.FailNow()
	}
	return store
}

func testEvent(
	t *testing.T, at time.Time, typ timebox.EventType, value int,
	status *string, labels map[string]string,
) *timebox.Event {
	t.Helper()

	data, err := json.Marshal(indexData{
		Value:  value,
		Status: status,
		Labels: labels,
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
				Labels: data.Labels,
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

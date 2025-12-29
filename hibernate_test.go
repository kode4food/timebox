package timebox_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

type (
	testHibernator struct {
		mu    sync.Mutex
		items map[string]*timebox.HibernateRecord
	}

	stubHibernator struct {
		record *timebox.HibernateRecord
		err    error
	}

	recordingHibernator struct {
		putCount int
		last     *timebox.HibernateRecord
	}
)

func TestHibernate(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Hibernator = newTestHibernator()

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("test", "hibernate")

	evs := []*timebox.Event{
		{
			Type:        EventIncremented,
			AggregateID: id,
			Timestamp:   time.Now(),
			Data:        json.RawMessage(`1`),
		},
		{
			Type:        EventIncremented,
			AggregateID: id,
			Timestamp:   time.Now(),
			Data:        json.RawMessage(`2`),
		},
	}

	err = store.AppendEvents(ctx, id, 0, evs)
	assert.NoError(t, err)

	err = store.Hibernate(ctx, id)
	assert.NoError(t, err)

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 2)

	aggregates, err := store.ListAggregates(ctx, id)
	assert.NoError(t, err)
	assert.Empty(t, aggregates)
}

func TestHibernatorEvents(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernator-events"

	ev := struct {
		Timestamp time.Time         `json:"timestamp"`
		Type      timebox.EventType `json:"type"`
		Data      json.RawMessage   `json:"data"`
	}{
		Timestamp: time.Now(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`1`),
	}
	raw, err := json.Marshal(ev)
	assert.NoError(t, err)

	storeCfg.Hibernator = stubHibernator{
		record: &timebox.HibernateRecord{
			Events: []json.RawMessage{
				json.RawMessage(raw), json.RawMessage(raw),
			},
			Snapshots: map[string]timebox.SnapshotRecord{},
		},
	}

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "hibernated")

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 2)
	assert.Equal(t, int64(0), events[0].Sequence)
	assert.Equal(t, int64(1), events[1].Sequence)
	assert.Equal(t, id, events[0].AggregateID)
}

func TestHibernatorSnapshot(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernator-snapshot"

	ev := struct {
		Timestamp time.Time         `json:"timestamp"`
		Type      timebox.EventType `json:"type"`
		Data      json.RawMessage   `json:"data"`
	}{
		Timestamp: time.Now(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`1`),
	}
	raw, err := json.Marshal(ev)
	assert.NoError(t, err)

	snapData, err := json.Marshal(&CounterState{Value: 5})
	assert.NoError(t, err)

	storeCfg.Hibernator = stubHibernator{
		record: &timebox.HibernateRecord{
			Events: []json.RawMessage{
				json.RawMessage(raw), json.RawMessage(raw),
			},
			Snapshots: map[string]timebox.SnapshotRecord{
				"snapshot": {
					Data:     json.RawMessage(snapData),
					Sequence: 1,
				},
			},
		},
	}

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("counter", "hibernated")

	var state CounterState
	result, err := store.GetSnapshot(ctx, id, &state)
	assert.NoError(t, err)
	assert.Equal(t, 5, state.Value)
	assert.Len(t, result.AdditionalEvents, 1)
	assert.Equal(t, int64(1), result.NextSequence)
}

func TestHibernatorSnapshotMissing(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernator-missing"
	storeCfg.Hibernator = stubHibernator{err: timebox.ErrHibernateNotFound}

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	var state CounterState
	snap, err := store.GetSnapshot(
		context.Background(), timebox.NewAggregateID("counter", "1"), &state,
	)
	assert.NoError(t, err)
	assert.NotNil(t, snap)
	assert.Len(t, snap.AdditionalEvents, 0)
	assert.Equal(t, int64(0), snap.NextSequence)
}

func TestHibernatorEventsMissing(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernator-missing-events"
	storeCfg.Hibernator = stubHibernator{err: timebox.ErrHibernateNotFound}

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	events, err := store.GetEvents(
		context.Background(), timebox.NewAggregateID("counter", "1"), 0,
	)
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestHibernatorEventsError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernator-events-error"
	storeCfg.Hibernator = stubHibernator{err: errors.New("boom")}

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	events, err := store.GetEvents(
		context.Background(), timebox.NewAggregateID("counter", "1"), 0,
	)
	assert.Error(t, err)
	assert.Nil(t, events)
}

func TestHibernatorSnapshotError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernator-snapshot-error"
	storeCfg.Hibernator = stubHibernator{err: errors.New("boom")}

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	var state CounterState
	snap, err := store.GetSnapshot(
		context.Background(), timebox.NewAggregateID("counter", "1"), &state,
	)
	assert.Error(t, err)
	assert.Nil(t, snap)
}

func TestHibernateEmpty(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernate-empty"

	recorder := &recordingHibernator{}
	storeCfg.Hibernator = recorder

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.Hibernate(
		context.Background(), timebox.NewAggregateID("order", "1"),
	)
	assert.NoError(t, err)
	assert.Equal(t, 0, recorder.putCount)
	assert.Nil(t, recorder.last)
}

func TestHibernateEventsOnly(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernate-events"

	recorder := &recordingHibernator{}
	storeCfg.Hibernator = recorder

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t, store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}))

	err = store.Hibernate(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, 1, recorder.putCount)
	assert.NotNil(t, recorder.last)
	assert.Len(t, recorder.last.Events, 1)
	assert.Len(t, recorder.last.Snapshots, 0)
}

func TestHibernateSnapshot(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Prefix = "hibernate-snapshot"

	recorder := &recordingHibernator{}
	storeCfg.Hibernator = recorder

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")

	err = store.PutSnapshot(ctx, id, map[string]int{"value": 3}, 2)
	assert.NoError(t, err)

	err = store.Hibernate(ctx, id)
	assert.NoError(t, err)
	assert.Equal(t, 1, recorder.putCount)
	assert.NotNil(t, recorder.last)
	assert.Len(t, recorder.last.Events, 0)
	assert.Equal(t, int64(2), recorder.last.Snapshots["snapshot"].Sequence)
}

func (s stubHibernator) Get(
	_ context.Context, _ timebox.AggregateID,
) (*timebox.HibernateRecord, error) {
	return s.record, s.err
}

func (s stubHibernator) Put(
	_ context.Context, _ timebox.AggregateID, _ *timebox.HibernateRecord,
) error {
	return nil
}

func (s stubHibernator) Delete(
	_ context.Context, _ timebox.AggregateID,
) error {
	return nil
}

func (r *recordingHibernator) Get(
	_ context.Context, _ timebox.AggregateID,
) (*timebox.HibernateRecord, error) {
	return nil, timebox.ErrHibernateNotFound
}

func (r *recordingHibernator) Put(
	_ context.Context, _ timebox.AggregateID, record *timebox.HibernateRecord,
) error {
	r.putCount++
	r.last = record
	return nil
}

func (r *recordingHibernator) Delete(
	_ context.Context, _ timebox.AggregateID,
) error {
	return nil
}

func newTestHibernator() *testHibernator {
	return &testHibernator{
		items: make(map[string]*timebox.HibernateRecord),
	}
}

func (h *testHibernator) Get(
	_ context.Context, id timebox.AggregateID,
) (*timebox.HibernateRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	record, ok := h.items[id.Join(":")]
	if !ok {
		return nil, timebox.ErrHibernateNotFound
	}
	return record, nil
}

func (h *testHibernator) Put(
	_ context.Context, id timebox.AggregateID, record *timebox.HibernateRecord,
) error {
	if record == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	h.items[id.Join(":")] = record
	return nil
}

func (h *testHibernator) Delete(
	_ context.Context, id timebox.AggregateID,
) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.items, id.Join(":"))
	return nil
}

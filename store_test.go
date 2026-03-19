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

type (
	fakePersistence struct {
		timebox.AlwaysReady
	}

	fakeReadyPersistence struct {
		fakePersistence
		readyCh chan struct{}
	}
)

func TestVersionConflictError(t *testing.T) {
	err := &timebox.VersionConflictError{
		ExpectedSequence: 0,
		ActualSequence:   5,
		NewEvents:        []*timebox.Event{{}, {}},
	}

	assert.Contains(t, err.Error(), "version conflict")
	assert.Contains(t, err.Error(), "expected sequence 0")
	assert.Contains(t, err.Error(), "but at 5")
}

func TestNewStoreValidate(t *testing.T) {
	store, err := timebox.NewStore(&fakePersistence{}, timebox.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NoError(t, store.Close())
}

func TestStoreReadyDefault(t *testing.T) {
	store, err := timebox.NewStore(&fakePersistence{}, timebox.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer func() { _ = store.Close() }()

	select {
	case <-store.Ready():
	default:
		t.Fatal("expected default store readiness to be immediate")
	}
}

func TestStoreWaitReady(t *testing.T) {
	p := &fakeReadyPersistence{readyCh: make(chan struct{})}
	store, err := timebox.NewStore(p, timebox.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer func() { _ = store.Close() }()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, store.WaitReady(ctx), context.DeadlineExceeded)

	close(p.readyCh)

	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	assert.NoError(t, store.WaitReady(ctx))
}

func TestStoreConfigAndStatus(t *testing.T) {
	cfg := timebox.Config{
		MaxRetries: 3,
		CacheSize:  5,
		Snapshot: timebox.SnapshotConfig{
			WorkerCount:  1,
			MaxQueueSize: 1,
		},
	}
	p := memory.NewPersistence(cfg)
	store, err := timebox.NewStore(p, cfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	assert.Equal(t, 3, store.Config().MaxRetries)

	id := timebox.NewAggregateID("order", "1")
	status := "active"
	_, err = p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Status:           &status,
		StatusAt:         "1700000000000",
		Events: []*timebox.Event{{
			Timestamp: time.Unix(0, 0).UTC(),
			Type:      "created",
			Data:      json.RawMessage(`{}`),
		}},
	})
	assert.NoError(t, err)

	got, err := store.GetAggregateStatus(id)
	assert.NoError(t, err)
	assert.Equal(t, status, got)
}

func TestStore(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("test", "1")

	ev := &timebox.Event{
		Type:        EventIncremented,
		AggregateID: id,
		Timestamp:   time.Now(),
		Data:        json.RawMessage(`5`),
	}

	err = store.AppendEvents(id, 0, []*timebox.Event{ev})
	assert.NoError(t, err)

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, EventIncremented, events[0].Type)
}

func TestAppendCopy(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("order", "1")
	other := timebox.NewAggregateID("other", "2")
	ev1 := &timebox.Event{
		Timestamp:   time.Now(),
		Sequence:    41,
		Type:        "event.first",
		AggregateID: other,
		Data:        json.RawMessage(`{"value":1}`),
	}
	ev2 := &timebox.Event{
		Timestamp:   time.Now(),
		Sequence:    42,
		Type:        "event.second",
		AggregateID: other,
		Data:        json.RawMessage(`{"value":2}`),
	}

	err = store.AppendEvents(id, 0, []*timebox.Event{ev1, ev2})
	assert.NoError(t, err)

	assert.Equal(t, int64(41), ev1.Sequence)
	assert.Equal(t, other, ev1.AggregateID)
	assert.Equal(t, int64(42), ev2.Sequence)
	assert.Equal(t, other, ev2.AggregateID)

	evs, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, evs, 2)
	assert.Equal(t, int64(0), evs[0].Sequence)
	assert.Equal(t, int64(1), evs[1].Sequence)
	assert.Equal(t, id, evs[0].AggregateID)
	assert.Equal(t, id, evs[1].AggregateID)
}

func TestNewStoreInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  timebox.Config
		err  error
	}{
		{
			name: "Negative MaxRetries",
			cfg: timebox.Config{
				MaxRetries: -1,
			},
			err: timebox.ErrInvalidMaxRetries,
		},
		{
			name: "Negative CacheSize",
			cfg: timebox.Config{
				CacheSize: -1,
			},
			err: timebox.ErrInvalidCacheSize,
		},
		{
			name: "Negative WorkerCount",
			cfg: timebox.Config{
				Snapshot: timebox.SnapshotConfig{
					WorkerCount: -1,
				},
			},
			err: timebox.ErrInvalidWorkerCount,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := timebox.NewStore(&fakePersistence{}, tc.cfg)
			assert.ErrorIs(t, err, tc.err)
			assert.Nil(t, store)
		})
	}
}

func TestAppendConflict(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("order", "1")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	err = store.AppendEvents(id, 1, []*timebox.Event{ev})
	assert.Error(t, err)
}

func TestArchiveUnsupported(t *testing.T) {
	store, err := timebox.NewStore(&fakePersistence{}, timebox.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("order", "1")

	err = store.Archive(id)
	assert.ErrorIs(t, err, timebox.ErrArchivingDisabled)

	err = store.ConsumeArchive(context.Background(), func(
		context.Context, *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.ErrorIs(t, err, timebox.ErrArchivingDisabled)

}

func TestListAggregates(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id1 := timebox.NewAggregateID("order", "1")
	id2 := timebox.NewAggregateID("order", "2")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t, store.AppendEvents(id1, 0, []*timebox.Event{ev}))
	assert.NoError(t, store.AppendEvents(id2, 0, []*timebox.Event{ev}))

	ids, err := store.ListAggregates(id1)
	assert.NoError(t, err)
	assert.Len(t, ids, 1)
	assert.Equal(t, id1, ids[0])
}

func TestGetEventsEmpty(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	events, err := store.GetEvents(timebox.NewAggregateID("counter", "1"), 0)
	assert.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestStoreTrimmedPlainAppend(t *testing.T) {
	server, store, err := newMemoryStore(timebox.Config{
		Snapshot: timebox.SnapshotConfig{TrimEvents: true},
	})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("order", "1")
	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      EventIncremented,
		Data:      json.RawMessage(`5`),
	}

	assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, EventIncremented, events[0].Type)
}

func TestStoreCombinedIndexing(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			withIndexedMemoryStore(t, trimEvents, combinedIndexer, func(
				ctx context.Context, store *timebox.Store,
			) {
				id := timebox.NewAggregateID("order", "1")

				assert.NoError(t,
					store.AppendEvents(id, 0, []*timebox.Event{
						combinedEvent(id, "active", "prod"),
					}),
				)

				statuses, err := store.ListAggregatesByStatus("active")
				assert.NoError(t, err)
				assert.Len(t, statuses, 1)
				assert.Equal(t, id, statuses[0].ID)

				values, err := store.ListLabelValues("env")
				assert.NoError(t, err)
				assert.Equal(t, []string{"prod"}, values)

				ids, err := store.ListAggregatesByLabel("env", "prod")
				assert.NoError(t, err)
				assert.Equal(t, []timebox.AggregateID{id}, ids)
			})
		})
	}
}

func withIndexedMemoryStore(
	t *testing.T, trimEvents bool, indexer timebox.Indexer,
	fn func(context.Context, *timebox.Store),
) {
	t.Helper()

	server, store, err := newMemoryStore(timebox.Config{
		Snapshot: timebox.SnapshotConfig{TrimEvents: trimEvents},
		Indexer:  indexer,
	})
	assert.NoError(t, err)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	fn(context.Background(), store)
}

func combinedEvent(
	id timebox.AggregateID, status string, env string,
) *timebox.Event {
	data, err := json.Marshal(map[string]string{
		"status": status,
		"env":    env,
	})
	if err != nil {
		panic(err)
	}

	return &timebox.Event{
		Timestamp:   time.Now(),
		Type:        EventIncremented,
		AggregateID: id,
		Data:        data,
	}
}

func combinedIndexer(events []*timebox.Event) []*timebox.Index {
	var res []*timebox.Index
	for _, ev := range events {
		data := map[string]string{}
		if err := json.Unmarshal(ev.Data, &data); err != nil {
			continue
		}

		status, ok := data["status"]
		if !ok {
			continue
		}

		res = append(res, &timebox.Index{
			Status: &status,
			Labels: map[string]string{"env": data["env"]},
		})
	}
	return res
}

func (f *fakePersistence) Close() error {
	return nil
}

func (f *fakePersistence) Append(
	timebox.AppendRequest,
) (*timebox.AppendResult, error) {
	return nil, nil
}

func (f *fakePersistence) LoadEvents(
	timebox.AggregateID, int64,
) (*timebox.EventsResult, error) {
	return &timebox.EventsResult{}, nil
}

func (f *fakePersistence) LoadSnapshot(
	timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	return &timebox.SnapshotRecord{}, nil
}

func (f *fakePersistence) SaveSnapshot(
	timebox.AggregateID, []byte, int64,
) error {
	return nil
}

func (f *fakePersistence) ListAggregates(
	timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	return nil, nil
}

func (f *fakePersistence) GetAggregateStatus(
	timebox.AggregateID,
) (string, error) {
	return "", nil
}

func (f *fakePersistence) ListAggregatesByStatus(
	string,
) ([]timebox.StatusEntry, error) {
	return nil, nil
}

func (f *fakePersistence) ListAggregatesByLabel(
	string, string,
) ([]timebox.AggregateID, error) {
	return nil, nil
}

func (f *fakePersistence) ListLabelValues(string) ([]string, error) {
	return nil, nil
}

func (f *fakeReadyPersistence) Ready() <-chan struct{} {
	return f.readyCh
}

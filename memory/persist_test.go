package memory_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
)

func TestNewStore(t *testing.T) {
	store, err := memory.NewStore(timebox.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NoError(t, store.Close())
}

func TestReady(t *testing.T) {
	p := memory.NewPersistence()
	select {
	case <-p.Ready():
	default:
		t.Fatal("Ready channel should be closed")
	}
}

func TestAppendAndLoadEvents(t *testing.T) {
	p := memory.NewPersistence()
	id := timebox.NewAggregateID("order", "1")

	res, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           testEvents("created", "updated"),
	})
	assert.NoError(t, err)
	assert.Nil(t, res)

	got, err := p.LoadEvents(id, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), got.StartSequence)
	assert.Equal(t,
		[]timebox.EventType{"created", "updated"}, eventTypes(got.Events),
	)
}

func TestAppendConflictTrailing(t *testing.T) {
	p := memory.NewPersistence()
	id := timebox.NewAggregateID("order", "1")

	_, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           testEvents("created", "updated"),
	})
	assert.NoError(t, err)

	res, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 1,
		Events:           testEvents("stale"),
	})
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, int64(2), res.ActualSequence)
	assert.Equal(t, []timebox.EventType{"updated"}, eventTypes(res.NewEvents))
}

func TestAppendStatusNoAt(t *testing.T) {
	p := memory.NewPersistence()
	id := timebox.NewAggregateID("order", "noat")
	status := "active"
	_, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Status:           &status,
		Events:           testEvents("created"),
	})
	assert.NoError(t, err)
	got, err := p.GetAggregateStatus(id)
	assert.NoError(t, err)
	assert.Equal(t, status, got)
}

func TestAppendLabelDeletion(t *testing.T) {
	p := memory.NewPersistence()
	id := timebox.NewAggregateID("order", "lbl")
	_, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Labels:           map[string]string{"env": "prod"},
		Events:           testEvents("created"),
	})
	assert.NoError(t, err)
	_, err = p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 1,
		Labels:           map[string]string{"env": ""},
		Events:           testEvents("updated"),
	})
	assert.NoError(t, err)
	vals, err := p.ListLabelValues("env")
	assert.NoError(t, err)
	assert.Empty(t, vals)
}

func TestSaveSnapshotTrimsEvents(t *testing.T) {
	p := memory.NewPersistence(timebox.Config{
		Snapshot: timebox.SnapshotConfig{TrimEvents: true},
	})
	id := timebox.NewAggregateID("order", "1")

	_, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           testEvents("one", "two"),
	})
	assert.NoError(t, err)

	err = p.SaveSnapshot(id, []byte(`{"value":1}`), 1)
	assert.NoError(t, err)

	snap, err := p.LoadSnapshot(id)
	assert.NoError(t, err)
	assert.Equal(t, json.RawMessage(`{"value":1}`), snap.Data)
	assert.Equal(t, int64(1), snap.Sequence)
	assert.Equal(t, []timebox.EventType{"two"}, eventTypes(snap.Events))

	evs, err := p.LoadEvents(id, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), evs.StartSequence)
	assert.Equal(t, []timebox.EventType{"two"}, eventTypes(evs.Events))
}

func TestSaveSnapshotStale(t *testing.T) {
	p := memory.NewPersistence()
	id := timebox.NewAggregateID("order", "stale")
	_, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           testEvents("one", "two"),
	})
	assert.NoError(t, err)
	err = p.SaveSnapshot(id, []byte(`{"v":2}`), 2)
	assert.NoError(t, err)
	// stale snapshot at earlier sequence should be ignored
	err = p.SaveSnapshot(id, []byte(`{"v":0}`), 0)
	assert.NoError(t, err)
	snap, err := p.LoadSnapshot(id)
	assert.NoError(t, err)
	assert.Equal(t, json.RawMessage(`{"v":2}`), snap.Data)
	assert.Equal(t, int64(2), snap.Sequence)
}

func TestLoadSnapshotMissing(t *testing.T) {
	p := memory.NewPersistence()
	snap, err := p.LoadSnapshot(timebox.NewAggregateID("none"))
	assert.NoError(t, err)
	assert.Equal(t, &timebox.SnapshotRecord{}, snap)
}

func TestGetAggregateStatusMissing(t *testing.T) {
	p := memory.NewPersistence()
	status, err := p.GetAggregateStatus(timebox.NewAggregateID("none"))
	assert.NoError(t, err)
	assert.Equal(t, "", status)
}

func TestAggregateQueries(t *testing.T) {
	p := memory.NewPersistence()
	status := "active"
	ts := time.Unix(1700000000, 0).UTC()
	first := timebox.NewAggregateID("order", "1")
	second := timebox.NewAggregateID("order", "2")

	_, err := p.Append(timebox.AppendRequest{
		ID:               first,
		ExpectedSequence: 0,
		Status:           &status,
		StatusAt:         "1700000000000",
		Labels: map[string]string{
			"env":    "prod",
			"region": "eu",
		},
		Events: testEvents("created"),
	})
	assert.NoError(t, err)

	_, err = p.Append(timebox.AppendRequest{
		ID:               second,
		ExpectedSequence: 0,
		Labels:           map[string]string{"env": "stage"},
		Events:           testEvents("created"),
	})
	assert.NoError(t, err)

	aggs, err := p.ListAggregates(timebox.NewAggregateID("order"))
	assert.NoError(t, err)
	assert.ElementsMatch(t, []timebox.AggregateID{first, second}, aggs)

	gotStatus, err := p.GetAggregateStatus(first)
	assert.NoError(t, err)
	assert.Equal(t, status, gotStatus)

	statuses, err := p.ListAggregatesByStatus(status)
	assert.NoError(t, err)
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        first,
		Timestamp: ts,
	}}, statuses)

	ids, err := p.ListAggregatesByLabel("env", "prod")
	assert.NoError(t, err)
	assert.Equal(t, []timebox.AggregateID{first}, ids)

	vals, err := p.ListLabelValues("env")
	assert.NoError(t, err)
	assert.Equal(t, []string{"prod", "stage"}, vals)
}

func TestArchiveLifecycle(t *testing.T) {
	p := memory.NewPersistence(timebox.Config{})
	id := timebox.NewAggregateID("order", "1")

	_, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           testEvents("created"),
	})
	assert.NoError(t, err)
	assert.NoError(t, p.SaveSnapshot(id, []byte(`{"value":1}`), 1))
	assert.NoError(t, p.Archive(id))

	var got *timebox.ArchiveRecord
	err = p.ConsumeArchive(context.Background(), func(
		_ context.Context, rec *timebox.ArchiveRecord,
	) error {
		got = rec
		return nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, id, got.AggregateID)
	assert.Equal(t, json.RawMessage(`{"value":1}`), got.SnapshotData)
	assert.Equal(t, int64(1), got.SnapshotSequence)
	assert.Equal(t, []timebox.EventType{"created"}, eventTypes(got.Events))

	aggs, err := p.ListAggregates(timebox.NewAggregateID("order"))
	assert.NoError(t, err)
	assert.Empty(t, aggs)
}

func TestArchiveMissingAggregate(t *testing.T) {
	p := memory.NewPersistence(timebox.Config{})
	err := p.Archive(timebox.NewAggregateID("order", "missing"))
	assert.NoError(t, err)
}

func TestArchiveErrors(t *testing.T) {
	p := memory.NewPersistence()
	err := p.ConsumeArchive(context.Background(), nil)
	assert.ErrorIs(t, err, timebox.ErrArchiveHandlerMissing)
}

func TestConsumeArchiveTimeout(t *testing.T) {
	p := memory.NewPersistence(timebox.Config{})
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	err := p.ConsumeArchive(ctx, func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestConsumeArchiveContextCancel(t *testing.T) {
	p := memory.NewPersistence(timebox.Config{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.ConsumeArchive(ctx, func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestArchiveHandlerErrorKeeps(t *testing.T) {
	p := memory.NewPersistence(timebox.Config{})
	id := timebox.NewAggregateID("order", "1")

	_, err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           testEvents("created"),
	})
	assert.NoError(t, err)
	assert.NoError(t, p.Archive(id))

	handlerErr := errors.New("handler failed")
	err = p.ConsumeArchive(context.Background(), func(
		_ context.Context, rec *timebox.ArchiveRecord,
	) error {
		assert.Equal(t, id, rec.AggregateID)
		return handlerErr
	})
	assert.ErrorIs(t, err, handlerErr)

	var got *timebox.ArchiveRecord
	err = p.ConsumeArchive(context.Background(), func(
		_ context.Context, rec *timebox.ArchiveRecord,
	) error {
		got = rec
		return nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, id, got.AggregateID)
}

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

func eventTypes(evs []*timebox.Event) []timebox.EventType {
	res := make([]timebox.EventType, 0, len(evs))
	for _, ev := range evs {
		res = append(res, ev.Type)
	}
	return res
}

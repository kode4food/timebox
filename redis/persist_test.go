package redis_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	tbredis "github.com/kode4food/timebox/redis"
)

func TestReady(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		select {
		case <-p.Ready():
		default:
			t.Fatal("Ready channel should be closed")
		}
	})
}

func TestNewPersistenceBadConfig(t *testing.T) {
	_, err := tbredis.NewPersistence(tbredis.Config{DB: -1})
	assert.ErrorIs(t, err, tbredis.ErrInvalidDB)
}

func TestConsumeArchiveServerDown(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	addr := server.Addr()

	p, err := tbredis.NewPersistence(tbredis.Config{Addr: addr})
	assert.NoError(t, err)
	defer func() { _ = p.Close() }()

	server.Close()
	err = p.ConsumeArchive(context.Background(), func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.Error(t, err)
}

func TestNewPersistencePingError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	addr := server.Addr()
	server.Close()

	p, err := newPersistence(tbredis.Config{Addr: addr})
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestNewStore(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := tbredis.NewStore(
		testConfig(server.Addr(), func(cfg *tbredis.Config) {
			cfg.Shard = "blue"
		}),
	)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NoError(t, store.Close())
}

func TestAppendLoadEvents(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		id := timebox.NewAggregateID("order", "1")

		res, err := p.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 0,
			Events: testEvents(
				"created", "updated",
			),
		})
		assert.NoError(t, err)
		assert.Nil(t, res)

		evs, err := p.LoadEvents(id, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), evs.StartSequence)
		assert.Equal(t,
			[]timebox.EventType{"created", "updated"}, eventTypes(evs.Events),
		)

		res, err = p.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 1,
			Events:           testEvents("stale"),
		})
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, int64(2), res.ActualSequence)
		assert.Equal(t, []timebox.EventType{"updated"}, eventTypes(res.NewEvents))
	})
}

func TestSnapshotListAggregates(t *testing.T) {
	withPersistence(t, func(cfg *tbredis.Config) {
		cfg.Timebox.Snapshot.TrimEvents = true
	}, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		id := timebox.NewAggregateID("order", "1")
		other := timebox.NewAggregateID("order", "2")

		_, err := p.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 0,
			Events:           testEvents("one", "two"),
		})
		assert.NoError(t, err)

		err = p.SaveSnapshot(id, []byte(`{"value":1}`), 1)
		assert.NoError(t, err)
		_, err = p.Append(timebox.AppendRequest{
			ID:               other,
			ExpectedSequence: 0,
			Events:           testEvents("created"),
		})
		assert.NoError(t, err)
		err = p.SaveSnapshot(other, []byte(`{"value":2}`), 1)
		assert.NoError(t, err)

		snap, err := p.LoadSnapshot(id)
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"value":1}`), snap.Data)
		assert.Equal(t, int64(1), snap.Sequence)
		assert.Equal(t, []timebox.EventType{"two"}, eventTypes(snap.Events))

		evs, err := p.LoadEvents(id, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), evs.StartSequence)

		ids, err := p.ListAggregates(id)
		assert.NoError(t, err)
		assert.Equal(t, []timebox.AggregateID{id}, ids)

		ids, err = p.ListAggregates(other)
		assert.NoError(t, err)
		assert.Equal(t, []timebox.AggregateID{other}, ids)
	})
}

func TestPersistenceIndexQueries(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		id := timebox.NewAggregateID("order", "1")
		status := "active"

		_, err := p.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 0,
			Status:           &status,
			StatusAt:         "1700000000000",
			Labels: map[string]string{
				"env:prod": `eu\west%1`,
			},
			Events: testEvents("created"),
		})
		assert.NoError(t, err)

		gotStatus, err := p.GetAggregateStatus(id)
		assert.NoError(t, err)
		assert.Equal(t, status, gotStatus)

		statuses, err := p.ListAggregatesByStatus(status)
		assert.NoError(t, err)
		assert.Equal(t, []timebox.StatusEntry{{
			ID:        id,
			Timestamp: time.Unix(1700000000, 0).UTC(),
		}}, statuses)

		ids, err := p.ListAggregatesByLabel("env:prod", `eu\west%1`)
		assert.NoError(t, err)
		assert.Equal(t, []timebox.AggregateID{id}, ids)

		vals, err := p.ListLabelValues("env:prod")
		assert.NoError(t, err)
		assert.Equal(t, []string{`eu\west%1`}, vals)
	})
}

func TestAggregateStatusMissing(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		status, err := p.GetAggregateStatus(timebox.NewAggregateID("missing"))
		assert.NoError(t, err)
		assert.Equal(t, "", status)
	})
}

func TestArchiveLifecycle(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		id := timebox.NewAggregateID("order", "1")

		_, err := p.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 0,
			Events:           testEvents("created"),
		})
		assert.NoError(t, err)
		err = p.SaveSnapshot(id, []byte(`{"value":1}`), 1)
		assert.NoError(t, err)
		assert.NoError(t, p.Archive(id))

		var got *timebox.ArchiveRecord
		err = p.ConsumeArchive(ctx, func(
			_ context.Context, rec *timebox.ArchiveRecord,
		) error {
			got = rec
			return nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, got)
		assert.Equal(t, id, got.AggregateID)
		assert.Equal(t, int64(1), got.SnapshotSequence)

		ids, err := p.ListAggregates(timebox.NewAggregateID("order"))
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestArchiveMissing(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		err := p.Archive(timebox.NewAggregateID("missing"))
		assert.NoError(t, err)
	})
}

func TestPersistenceArchiveErrors(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, _ *redis.Client,
	) {
		err := p.ConsumeArchive(ctx, nil)
		assert.ErrorIs(t, err, timebox.ErrArchiveHandlerMissing)
	})
}

func TestArchiveMalformed(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		streamKey := tbredis.DefaultPrefix + ":archive"
		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{"bad": "data"},
		}).Result()
		assert.NoError(t, err)

		err = p.ConsumeArchive(ctx, func(
			_ context.Context, _ *timebox.ArchiveRecord,
		) error {
			return nil
		})
		assert.ErrorIs(t, err, timebox.ErrArchiveRecordMalformed)
	})
}

func TestArchivePayloadJSON(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		streamKey := tbredis.DefaultPrefix + ":archive"
		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{"payload": "{bad json"},
		}).Result()
		assert.NoError(t, err)

		err = p.ConsumeArchive(ctx, func(
			_ context.Context, _ *timebox.ArchiveRecord,
		) error {
			return nil
		})
		assert.Error(t, err)
	})
}

func TestArchivePayloadBytes(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		payload := []byte(`{"id":"order:bytes","snap":"","seq":0,"events":[]}`)
		streamKey := tbredis.DefaultPrefix + ":archive"
		_, err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{"payload": payload},
		}).Result()
		assert.NoError(t, err)

		var handled *timebox.ArchiveRecord
		err = p.ConsumeArchive(ctx, func(
			_ context.Context, record *timebox.ArchiveRecord,
		) error {
			handled = record
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t,
			timebox.NewAggregateID("order", "bytes"), handled.AggregateID,
		)
	})
}

func TestArchiveHandlerError(t *testing.T) {
	withPersistence(t, nil, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		id := timebox.NewAggregateID("order", "handler-error")

		_, err := p.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 0,
			Events:           testEvents("created"),
		})
		assert.NoError(t, err)
		assert.NoError(t, p.Archive(id))

		handlerErr := errors.New("handler failed")
		err = p.ConsumeArchive(ctx, func(
			_ context.Context, _ *timebox.ArchiveRecord,
		) error {
			return handlerErr
		})
		assert.ErrorIs(t, err, handlerErr)

		entries, err := client.XRange(ctx,
			tbredis.DefaultPrefix+":archive", "-", "+",
		).Result()
		assert.NoError(t, err)
		assert.Len(t, entries, 1)
	})
}

func TestPersistenceCorruptEvents(t *testing.T) {
	withPersistence(t, func(cfg *tbredis.Config) {
		cfg.Prefix = "corrupt"
	}, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		id := timebox.NewAggregateID("order", "1")
		err := client.RPush(ctx,
			"corrupt:"+id.Join(":")+":events", "not-json",
		).Err()
		assert.NoError(t, err)

		_, err = p.LoadEvents(id, 0)
		assert.Error(t, err)
	})
}

func TestCorruptSnapshot(t *testing.T) {
	withPersistence(t, func(cfg *tbredis.Config) {
		cfg.Prefix = "corrupt-snapshot"
	}, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		id := timebox.NewAggregateID("order", "1")
		err := client.Set(ctx,
			"corrupt-snapshot:"+id.Join(":")+":snapshot:val", "not-json", 0,
		).Err()
		assert.NoError(t, err)

		snap, err := p.LoadSnapshot(id)
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage("not-json"), snap.Data)
	})
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

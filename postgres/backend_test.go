package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

func TestNewStoreBadTimeboxConfig(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		b, err := postgres.Open(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = b.Close() }()

		store, err := b.NewStore(timebox.Config{MaxRetries: -1})
		assert.ErrorIs(t, err, timebox.ErrInvalidMaxRetries)
		assert.Nil(t, store)
	})
}

func TestEventRow(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		b, err := postgres.Open(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = b.Close() }()

		store, err := b.NewStore()
		if !assert.NoError(t, err) {
			return
		}

		id := timebox.NewAggregateID("order", "row")
		ev := testEvent(t, time.Unix(1_700_000_000, 123).UTC(), "a", "dev", 1)
		if !assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{ev}),
		) {
			return
		}

		pool, err := pgxpool.New(ctx, cfg.URL)
		if !assert.NoError(t, err) {
			return
		}
		defer pool.Close()

		var seq int64
		var at int64
		var typ string
		var data string
		err = pool.QueryRow(ctx, `
			SELECT sequence, event_at, event_type, data
			FROM timebox_events
			WHERE store = $1
		`, cfg.Prefix).Scan(&seq, &at, &typ, &data)
		if !assert.NoError(t, err) {
			return
		}

		var want any
		var got any
		if !assert.NoError(t, json.Unmarshal(ev.Data, &want)) {
			return
		}
		if !assert.NoError(t, json.Unmarshal([]byte(data), &got)) {
			return
		}
		assert.Equal(t, int64(0), seq)
		assert.Equal(t, ev.Timestamp.UnixNano(), at)
		assert.Equal(t, string(ev.Type), typ)
		assert.Equal(t, want, got)
	})
}

func TestNewStoreBadConfig(t *testing.T) {
	_, err := postgres.Open(postgres.Config{MaxConns: -1})
	assert.ErrorIs(t, err, postgres.ErrInvalidMaxConns)
}

func TestAppendConflictOrder(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		b, err := postgres.Open(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = b.Close() }()
		first := timebox.NewAggregateID("order", "z")
		second := timebox.NewAggregateID("order", "a")
		reqs := []timebox.AppendRequest{
			{ID: first, ExpectedSequence: 1},
			{ID: second, ExpectedSequence: 1},
		}
		var conflict *timebox.VersionConflictError
		if assert.ErrorAs(t, b.Append(reqs...), &conflict) {
			assert.Equal(t, first, conflict.ID)
		}
		assert.Equal(t, first, reqs[0].ID)
		ids, err := b.ListAggregates("")
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})
}

func TestOpenBadConfig(t *testing.T) {
	_, err := postgres.Open(postgres.Config{MaxConns: -1})
	assert.ErrorIs(t, err, postgres.ErrInvalidMaxConns)
}

func TestOpenBadURL(t *testing.T) {
	_, err := postgres.Open(postgres.Config{
		URL:      "postgres://localhost:1/bad?sslmode=disable",
		Prefix:   "test",
		MaxConns: 4,
	})
	assert.Error(t, err)
}

func TestClosedBackend(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		b, err := postgres.Open(cfg)
		if !assert.NoError(t, err) {
			return
		}
		assert.NoError(t, b.Close())

		id := timebox.NewAggregateID("order", "closed")

		_, err = b.LoadEvents(timebox.LoadEventsRequest{
			ID:      id,
			FromSeq: 0,
		})
		assert.Error(t, err)

		_, err = b.LoadSnapshot(timebox.LoadSnapshotRequest{
			ID: id,
		})
		assert.Error(t, err)

		err = b.SaveSnapshot(timebox.SnapshotRequest{
			ID:       id,
			Data:     []byte("{}"),
			Sequence: 0,
		})
		assert.Error(t, err)

		_, err = b.ListAggregates("")
		assert.Error(t, err)

		_, err = b.ListAggregatesByStatus("x")
		assert.Error(t, err)

		_, err = b.ListAggregatesByTag("v")
		assert.Error(t, err)

	})
}

func TestOpenInvalidURL(t *testing.T) {
	_, err := postgres.Open(postgres.Config{
		URL:      "://",
		Prefix:   "test",
		MaxConns: 4,
	})
	assert.Error(t, err)
}

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

		pool := schemaPool(t, ctx, cfg)
		defer pool.Close()

		var seq int64
		var at int64
		var typ string
		var data []byte
		err = pool.QueryRow(ctx, `
			SELECT sequence, event_at, event_type, data
			FROM timebox_events
		`).Scan(&seq, &at, &typ, &data)
		if !assert.NoError(t, err) {
			return
		}

		var want any
		var got any
		if !assert.NoError(t, json.Unmarshal(ev.Data, &want)) {
			return
		}
		if !assert.NoError(t, json.Unmarshal(data, &got)) {
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

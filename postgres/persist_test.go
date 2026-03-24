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

func TestEventRow(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

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
	_, err := postgres.NewStore(postgres.Config{MaxConns: -1})
	assert.ErrorIs(t, err, postgres.ErrInvalidMaxConns)
}

func TestNewPersistenceBadConfig(t *testing.T) {
	_, err := postgres.NewPersistence(postgres.Config{MaxConns: -1})
	assert.ErrorIs(t, err, postgres.ErrInvalidMaxConns)
}

func TestNewPersistenceBadURL(t *testing.T) {
	_, err := postgres.NewPersistence(postgres.Config{
		URL:      "postgres://localhost:1/bad?sslmode=disable",
		Prefix:   "test",
		MaxConns: 4,
	})
	assert.Error(t, err)
}

func TestClosedPersistence(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		assert.NoError(t, p.Close())

		id := timebox.NewAggregateID("order", "closed")

		_, err = p.LoadEvents(id, 0)
		assert.Error(t, err)

		_, err = p.LoadSnapshot(id)
		assert.Error(t, err)

		err = p.SaveSnapshot(id, []byte("{}"), 0)
		assert.Error(t, err)

		_, err = p.ListAggregates(nil)
		assert.Error(t, err)

		_, err = p.ListAggregatesByStatus("x")
		assert.Error(t, err)

		_, err = p.ListAggregatesByLabel("k", "v")
		assert.Error(t, err)

		_, err = p.ListLabelValues("k")
		assert.Error(t, err)
	})
}

func TestNewPersistenceInvalidURL(t *testing.T) {
	_, err := postgres.NewPersistence(postgres.Config{
		URL:      "://",
		Prefix:   "test",
		MaxConns: 4,
	})
	assert.Error(t, err)
}

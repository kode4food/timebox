package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

func TestTagRow(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		cfg.Timebox.Indexer = statusEnvIndexer(t)
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		store, err := timebox.NewStore(p)
		if !assert.NoError(t, err) {
			return
		}

		id := timebox.NewAggregateID("order", "row")
		ev := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(), "active", "dev", 1,
		)
		if !assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev})) {
			return
		}

		pool, err := pgxpool.New(ctx, cfg.URL)
		if !assert.NoError(t, err) {
			return
		}
		defer pool.Close()

		var tag string
		err = pool.QueryRow(ctx, `
			SELECT tag
			FROM timebox_tags
			WHERE store = $1
		`, cfg.Prefix).Scan(&tag)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, "dev", tag)
	})
}

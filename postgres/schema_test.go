package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox/postgres"
)

func TestPersistenceSchema(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		pool, err := pgxpool.New(ctx, cfg.URL)
		if !assert.NoError(t, err) {
			return
		}
		defer pool.Close()

		var colCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'timebox_events'
			  AND column_name IN (
			      'aggregate_kind', 'aggregate_id'
			  )
		`).Scan(&colCount)
		if !assert.NoError(t, err) {
			return
		}
		assert.Zero(t, colCount)

		var fkCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			WHERE t.relname IN (
				'timebox_events', 'timebox_snapshot'
			) AND c.contype = 'f'
		`).Scan(&fkCount)
		if !assert.NoError(t, err) {
			return
		}
		assert.Zero(t, fkCount)

		var idxCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND indexname = 'timebox_events_lookup_idx'
		`).Scan(&idxCount)
		if !assert.NoError(t, err) {
			return
		}
		assert.Zero(t, idxCount)
	})
}

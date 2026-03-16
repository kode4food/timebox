package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaTimeout = 60 * time.Second

var schemaStatements = func() []string {
	stmts := []string{
		`
CREATE TABLE IF NOT EXISTS timebox_index (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	aggregate_parts TEXT[] NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	status_at BIGINT NOT NULL DEFAULT 0,
	labels JSONB NOT NULL DEFAULT '{}'::jsonb,
	PRIMARY KEY (store, aggregate_key)
)`,
		`
CREATE INDEX IF NOT EXISTS timebox_index_status_idx
	ON timebox_index (
		store, status, status_at, aggregate_key
	)
`,
		`
CREATE TABLE IF NOT EXISTS timebox_events (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	sequence BIGINT NOT NULL,
	data TEXT NOT NULL,
	PRIMARY KEY (store, aggregate_key, sequence)
)`,
		`
CREATE TABLE IF NOT EXISTS timebox_snapshot (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	base_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_data TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (store, aggregate_key)
)`,
		// Drop legacy monolithic append function if present
		`DROP FUNCTION IF EXISTS timebox_append(
	TEXT, TEXT, TEXT[], BIGINT, TEXT, BIGINT, JSONB, TEXT[]
)`,
	}
	for _, spec := range appendFunctions {
		stmts = append(stmts, buildAppendFunctionSQL(spec))
	}
	return stmts
}()

func initSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, stmt := range schemaStatements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

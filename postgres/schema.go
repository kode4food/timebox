package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultSchemaTimeout = 60 * time.Second

var schemaStatements = func() []string {
	stmts := []string{
		`
CREATE TABLE IF NOT EXISTS timebox_statuses (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	aggregate_parts TEXT[] NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	status_at BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (store, aggregate_key)
)`,
		`
CREATE INDEX IF NOT EXISTS timebox_statuses_idx
	ON timebox_statuses (
		store, status, status_at, aggregate_key
	)
`,
		`
CREATE TABLE IF NOT EXISTS timebox_labels (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	label TEXT NOT NULL,
	value TEXT NOT NULL,
	PRIMARY KEY (store, aggregate_key, label)
)`,
		`
CREATE INDEX IF NOT EXISTS timebox_labels_value_idx
	ON timebox_labels (
		store, label, value, aggregate_key
	)
`,
		`
CREATE TABLE IF NOT EXISTS timebox_events (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	sequence BIGINT NOT NULL,
	event_at BIGINT NOT NULL,
	event_type TEXT NOT NULL,
	data TEXT NOT NULL,
	PRIMARY KEY (store, aggregate_key, sequence)
)`,
		`
CREATE TABLE IF NOT EXISTS timebox_snapshots (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	base_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_data TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (store, aggregate_key)
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

package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultSchemaTimeout = 60 * time.Second

var tableStatements = []string{
	`
CREATE TABLE IF NOT EXISTS timebox_statuses (
	aggregate_key TEXT NOT NULL,
	aggregate_parts TEXT[] NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	status_at BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (aggregate_key)
)`,
	`
CREATE INDEX IF NOT EXISTS timebox_statuses_status_idx
	ON timebox_statuses (
		status, status_at, aggregate_key
	)
`,
	`
CREATE INDEX IF NOT EXISTS timebox_statuses_type_idx
	ON timebox_statuses (
		(aggregate_parts[1]), aggregate_key
	)
`,
	`
CREATE TABLE IF NOT EXISTS timebox_tags (
	aggregate_key TEXT NOT NULL,
	tag TEXT NOT NULL,
	PRIMARY KEY (aggregate_key, tag)
)`,
	`
CREATE INDEX IF NOT EXISTS timebox_tags_lookup_idx
	ON timebox_tags (
		tag, aggregate_key
	)
`,
	`
CREATE TABLE IF NOT EXISTS timebox_events (
	aggregate_key TEXT NOT NULL,
	sequence BIGINT NOT NULL,
	event_at BIGINT NOT NULL,
	event_type TEXT NOT NULL,
	data BYTEA NOT NULL,
	PRIMARY KEY (aggregate_key, sequence)
)`,
	`
CREATE TABLE IF NOT EXISTS timebox_snapshots (
	aggregate_key TEXT NOT NULL,
	base_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_data BYTEA NOT NULL DEFAULT '',
	PRIMARY KEY (aggregate_key)
)`,
	appendFunctionSQL,
}

func initSchema(
	ctx context.Context, pool *pgxpool.Pool, schema string,
) error {
	_, err := pool.Exec(ctx,
		"CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize(),
	)
	if err != nil {
		return err
	}
	for _, stmt := range tableStatements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

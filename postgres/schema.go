package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultSchemaTimeout = 60 * time.Second

var schemaStatements = []string{
	sqlStatusesTable,
	sqlStatusesStatusIdx,
	sqlStatusesTypeIdx,
	sqlTagsTable,
	sqlTagsLookupIdx,
	sqlEventsTable,
	sqlSnapshotsTable,
	sqlAppendFunction,
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
	for _, stmt := range schemaStatements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

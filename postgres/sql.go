package postgres

import _ "embed"

var (
	//go:embed sql/statuses-table.sql
	sqlStatusesTable string

	//go:embed sql/statuses-status-idx.sql
	sqlStatusesStatusIdx string

	//go:embed sql/statuses-type-idx.sql
	sqlStatusesTypeIdx string

	//go:embed sql/tags-table.sql
	sqlTagsTable string

	//go:embed sql/tags-lookup-idx.sql
	sqlTagsLookupIdx string

	//go:embed sql/events-table.sql
	sqlEventsTable string

	//go:embed sql/snapshots-table.sql
	sqlSnapshotsTable string

	//go:embed sql/append-function.sql
	sqlAppendFunction string

	//go:embed sql/append.sql
	sqlAppend string

	//go:embed sql/lock-key.sql
	sqlLockKey string

	//go:embed sql/lock-keys.sql
	sqlLockKeys string

	//go:embed sql/check-sequence.sql
	sqlCheckSequence string

	//go:embed sql/insert-aggregate.sql
	sqlInsertAggregate string

	//go:embed sql/list-aggregates.sql
	sqlListAggregates string

	//go:embed sql/list-aggregates-by-type.sql
	sqlListAggregatesByType string

	//go:embed sql/get-events.sql
	sqlGetEvents string

	//go:embed sql/get-snapshot.sql
	sqlGetSnapshot string

	//go:embed sql/snapshot-base-seq.sql
	sqlSnapshotBaseSeq string

	//go:embed sql/snapshot-state.sql
	sqlSnapshotState string

	//go:embed sql/put-snapshot.sql
	sqlPutSnapshot string

	//go:embed sql/trim-events.sql
	sqlTrimEvents string

	//go:embed sql/get-status.sql
	sqlGetStatus string

	//go:embed sql/list-by-status.sql
	sqlListByStatus string

	//go:embed sql/list-by-tag.sql
	sqlListByTag string
)

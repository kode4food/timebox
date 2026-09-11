CREATE TABLE IF NOT EXISTS timebox_snapshots (
	aggregate_key TEXT NOT NULL,
	base_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_data BYTEA NOT NULL DEFAULT '',
	PRIMARY KEY (aggregate_key)
)

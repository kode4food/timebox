CREATE TABLE IF NOT EXISTS timebox_events (
	aggregate_key TEXT NOT NULL,
	sequence BIGINT NOT NULL,
	event_at BIGINT NOT NULL,
	event_type TEXT NOT NULL,
	data BYTEA NOT NULL,
	PRIMARY KEY (aggregate_key, sequence)
)

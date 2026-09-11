CREATE TABLE IF NOT EXISTS timebox_statuses (
	aggregate_key TEXT NOT NULL,
	aggregate_parts TEXT[] NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	status_at BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (aggregate_key)
)

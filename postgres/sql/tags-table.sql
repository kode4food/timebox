CREATE TABLE IF NOT EXISTS timebox_tags (
	aggregate_key TEXT NOT NULL,
	tag TEXT NOT NULL,
	PRIMARY KEY (aggregate_key, tag)
)

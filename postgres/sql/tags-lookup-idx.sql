CREATE INDEX IF NOT EXISTS timebox_tags_lookup_idx
	ON timebox_tags (
		tag, aggregate_key
	)

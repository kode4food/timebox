CREATE INDEX IF NOT EXISTS timebox_statuses_type_idx
	ON timebox_statuses (
		(aggregate_parts[1]), aggregate_key
	)

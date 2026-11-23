CREATE INDEX IF NOT EXISTS timebox_statuses_status_type_idx
	ON timebox_statuses (
		status, (aggregate_parts[1]), status_at, aggregate_key
	)

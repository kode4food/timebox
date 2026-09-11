CREATE INDEX IF NOT EXISTS timebox_statuses_status_idx
	ON timebox_statuses (
		status, status_at, aggregate_key
	)

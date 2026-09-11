INSERT INTO timebox_statuses (
	aggregate_key, aggregate_parts
) VALUES ($1, $2)
ON CONFLICT (aggregate_key) DO NOTHING

SELECT aggregate_parts
FROM timebox_statuses
WHERE status = $1
	AND aggregate_parts[1] = $2
	AND starts_with(aggregate_parts[2], $3)
ORDER BY aggregate_key

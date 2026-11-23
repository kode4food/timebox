SELECT aggregate_parts, status_at
FROM timebox_statuses
WHERE status = $1
	AND aggregate_parts[1] = $2
	AND (status_at, aggregate_key) < ($3, $4)
ORDER BY status_at DESC, aggregate_key DESC
LIMIT $5

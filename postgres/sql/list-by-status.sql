SELECT aggregate_parts, status_at
FROM timebox_statuses
WHERE status = $1
	AND ($2 = '' OR aggregate_parts[1] = $2)
	AND ($3 = '' OR starts_with(aggregate_parts[2], $3))
	AND ($4::bigint IS NULL OR status_at <= $4)
ORDER BY status_at

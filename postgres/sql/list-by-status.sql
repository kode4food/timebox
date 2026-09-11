SELECT aggregate_parts, status_at
FROM timebox_statuses
WHERE status = $1
ORDER BY status_at

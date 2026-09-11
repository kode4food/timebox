SELECT sequence, event_at, event_type, data
FROM timebox_events
WHERE aggregate_key = $1
  AND sequence >= $2
ORDER BY sequence

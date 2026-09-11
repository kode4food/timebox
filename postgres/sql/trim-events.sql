DELETE FROM timebox_events
WHERE aggregate_key = $1
  AND sequence < $2

SELECT 1 FROM timebox_statuses
WHERE aggregate_key = $1
FOR UPDATE

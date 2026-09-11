-- COLLATE "C" matches the Go sort, so both agree on lock order
SELECT 1 FROM timebox_statuses
WHERE aggregate_key = ANY($1)
ORDER BY aggregate_key COLLATE "C"
FOR UPDATE

UPDATE timebox_statuses
SET status = '', status_at = 0, deleted = TRUE
WHERE aggregate_key = $1

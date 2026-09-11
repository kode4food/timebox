-- Asserts a sequence without mutating, shaped like timebox_append so both
-- scan the same way
SELECT $2::bigint = seq AS success, seq AS actual_sequence
FROM (
	SELECT GREATEST(
		COALESCE((
			SELECT e.sequence + 1
			FROM timebox_events e
			WHERE e.aggregate_key = $1
			ORDER BY e.sequence DESC
			LIMIT 1
		), 0),
		COALESCE((
			SELECT s.snapshot_seq
			FROM timebox_snapshots s
			WHERE s.aggregate_key = $1
		), 0)
	) AS seq
) t

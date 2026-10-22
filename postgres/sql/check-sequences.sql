-- One statement reads every sequence from one snapshot, so the checks agree
-- without a transaction or locks
SELECT GREATEST(
	COALESCE((
		SELECT e.sequence + 1
		FROM timebox_events e
		WHERE e.aggregate_key = k.key
		ORDER BY e.sequence DESC
		LIMIT 1
	), 0),
	COALESCE((
		SELECT s.snapshot_seq
		FROM timebox_snapshots s
		WHERE s.aggregate_key = k.key
	), 0)
) AS actual_sequence
FROM unnest($1::text[]) WITH ORDINALITY AS k(key, ord)
ORDER BY k.ord

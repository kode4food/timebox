-- STABLE runs on the caller's snapshot, so one statement checking many keys
-- still reads every sequence at one point in time
CREATE OR REPLACE FUNCTION timebox_current_sequence(
	p_aggregate_key TEXT
) RETURNS BIGINT AS $$
	SELECT GREATEST(
		COALESCE((
			SELECT e.sequence + 1
			FROM timebox_events e
			WHERE e.aggregate_key = p_aggregate_key
			ORDER BY e.sequence DESC
			LIMIT 1
		), 0),
		COALESCE((
			SELECT s.snapshot_seq
			FROM timebox_snapshots s
			WHERE s.aggregate_key = p_aggregate_key
		), 0)
	)
$$ LANGUAGE sql STABLE

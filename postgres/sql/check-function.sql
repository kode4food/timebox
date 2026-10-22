CREATE OR REPLACE FUNCTION timebox_check_sequence(
	p_aggregate_key TEXT,
	p_expected_sequence BIGINT
) RETURNS VOID AS $$
DECLARE
	v_current_seq BIGINT;
BEGIN
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
	INTO v_current_seq;

	IF p_expected_sequence <> v_current_seq THEN
		RAISE EXCEPTION 'timebox version conflict'
			USING ERRCODE = 'TB409', DETAIL = v_current_seq::text;
	END IF;
END;
$$ LANGUAGE plpgsql

CREATE OR REPLACE FUNCTION timebox_check_sequence(
	p_aggregate_key TEXT,
	p_expected_sequence BIGINT
) RETURNS VOID AS $$
DECLARE
	v_current_seq BIGINT := timebox_current_sequence(p_aggregate_key);
BEGIN
	IF p_expected_sequence <> v_current_seq THEN
		RAISE EXCEPTION 'timebox version conflict'
			USING ERRCODE = 'TB409', DETAIL = v_current_seq::text;
	END IF;
END;
$$ LANGUAGE plpgsql

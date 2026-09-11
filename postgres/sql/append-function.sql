CREATE OR REPLACE FUNCTION timebox_append(
	p_aggregate_key TEXT,
	p_aggregate_parts TEXT[],
	p_expected_sequence BIGINT,
	p_status TEXT,
	p_status_at BIGINT,
	p_tags TEXT[],
	p_tag_adds BOOLEAN[],
	p_event_ats BIGINT[],
	p_event_types TEXT[],
	p_event_data BYTEA[]
) RETURNS TABLE(
	success BOOLEAN,
	actual_sequence BIGINT
) AS $$
DECLARE
	v_base_seq BIGINT := 0;
	v_snapshot_seq BIGINT := 0;
	v_next_seq BIGINT := 0;
	v_current_seq BIGINT := 0;
	v_event_count BIGINT := COALESCE(array_length(p_event_data, 1), 0);
BEGIN
	IF p_expected_sequence = 0 THEN
		INSERT INTO timebox_statuses (
			aggregate_key, aggregate_parts
		) VALUES (
			p_aggregate_key, p_aggregate_parts
		)
		ON CONFLICT (aggregate_key) DO NOTHING;
	END IF;

	SELECT COALESCE(s.base_seq, 0),
	       COALESCE(s.snapshot_seq, 0),
	       COALESCE((
	           SELECT e.sequence + 1
	           FROM timebox_events e
	           WHERE e.aggregate_key = p_aggregate_key
	           ORDER BY e.sequence DESC
	           LIMIT 1
	       ), COALESCE(s.base_seq, 0))
	INTO v_base_seq, v_snapshot_seq, v_next_seq
	FROM timebox_statuses i
	LEFT JOIN timebox_snapshots s
	  ON s.aggregate_key = i.aggregate_key
	WHERE i.aggregate_key = p_aggregate_key
	FOR UPDATE OF i;

	IF NOT FOUND THEN
		success := FALSE;
		actual_sequence := 0;
		RETURN NEXT;
		RETURN;
	END IF;

	v_current_seq := GREATEST(v_next_seq, v_snapshot_seq);
	IF p_expected_sequence <> v_current_seq THEN
		success := FALSE;
		actual_sequence := v_current_seq;
		RETURN NEXT;
		RETURN;
	END IF;

	INSERT INTO timebox_events (
		aggregate_key, sequence, event_at, event_type, data
	)
	SELECT p_aggregate_key,
		p_expected_sequence + ev.ord - 1,
		ev.event_at, ev.event_type, ev.data
	FROM unnest(
		COALESCE(p_event_ats, ARRAY[]::BIGINT[]),
		COALESCE(p_event_types, ARRAY[]::TEXT[]),
		COALESCE(p_event_data, ARRAY[]::BYTEA[])
	) WITH ORDINALITY AS ev(event_at, event_type, data, ord);

	IF p_status IS NOT NULL THEN
		UPDATE timebox_statuses
		SET status = p_status,
		    status_at = COALESCE(p_status_at, 0)
		WHERE aggregate_key = p_aggregate_key;
	END IF;

	IF COALESCE(array_length(p_tags, 1), 0) > 0 THEN
		DELETE FROM timebox_tags ti
		USING unnest(p_tags, p_tag_adds) AS item(tag, enabled)
		WHERE ti.aggregate_key = p_aggregate_key
		  AND ti.tag = item.tag
		  AND NOT item.enabled;

		INSERT INTO timebox_tags (
			aggregate_key, tag
		)
		SELECT p_aggregate_key, item.tag
		FROM unnest(p_tags, p_tag_adds) AS item(tag, enabled)
		WHERE item.enabled
		ON CONFLICT (aggregate_key, tag) DO NOTHING;
	END IF;

	success := TRUE;
	actual_sequence := v_current_seq + v_event_count;
	RETURN NEXT;
END;
$$ LANGUAGE plpgsql

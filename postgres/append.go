package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/kode4food/timebox"
)

type appendFunctionSpec struct {
	name   string
	status bool
	labels bool
}

var appendFunctions = []appendFunctionSpec{
	{name: "timebox_append_plain"},
	{name: "timebox_append_status", status: true},
	{name: "timebox_append_labels", labels: true},
	{
		name:   "timebox_append_status_labels",
		status: true,
		labels: true,
	},
}

const (
	appendPlainQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_plain($1, $2, $3, $4, $5::text[])
	`

	appendStatusQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_status(
			$1, $2, $3, $4, $5, $6, $7::text[]
		)
	`

	appendLabelsQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_labels(
			$1, $2, $3, $4, $5::jsonb, $6::text[]
		)
	`

	appendStatusLabelsQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_status_labels(
			$1, $2, $3, $4, $5, $6, $7::jsonb, $8::text[]
		)
	`
)

// Append appends events if the expected sequence matches
func (p *Persistence) Append(
	req timebox.AppendRequest,
) (*timebox.AppendResult, error) {
	ctx := context.Background()
	key, parts, err := aggregateKey(req.ID)
	if err != nil {
		return nil, err
	}
	raw, err := p.encodeEvents(req.Events)
	if err != nil {
		return nil, err
	}

	var status any
	var statusAt int64
	if req.Status != nil {
		status = *req.Status
		if req.StatusAt != "" {
			v, err := strconv.ParseInt(req.StatusAt, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: %s",
					timebox.ErrUnexpectedResult, err,
				)
			}
			statusAt = v
		}
	}

	var success bool
	var actualSeq int64
	var newEvents []string
	var lblJSON string
	if len(req.Labels) > 0 {
		b, err := json.Marshal(req.Labels)
		if err != nil {
			return nil, err
		}
		lblJSON = string(b)
	}

	switch {
	case req.Status != nil && len(req.Labels) > 0:
		err = p.pool.QueryRow(ctx, appendStatusLabelsQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			status, statusAt, lblJSON, raw,
		).Scan(&success, &actualSeq, &newEvents)
	case req.Status != nil:
		err = p.pool.QueryRow(ctx, appendStatusQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			status, statusAt, raw,
		).Scan(&success, &actualSeq, &newEvents)
	case len(req.Labels) > 0:
		err = p.pool.QueryRow(ctx, appendLabelsQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			lblJSON, raw,
		).Scan(&success, &actualSeq, &newEvents)
	default:
		err = p.pool.QueryRow(ctx, appendPlainQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			raw,
		).Scan(&success, &actualSeq, &newEvents)
	}
	if err != nil {
		return nil, err
	}
	if success {
		return nil, nil
	}
	res, err := p.decodeEvents(newEvents)
	if err != nil {
		return nil, err
	}
	return &timebox.AppendResult{
		ActualSequence: actualSeq,
		NewEvents:      res,
	}, nil
}

func (p *Persistence) encodeEvents(evs []*timebox.Event) ([]string, error) {
	return timebox.EncodeAll(timebox.JSONEvent, evs)
}

func (p *Persistence) decodeEvents(data []string) ([]*timebox.Event, error) {
	return timebox.DecodeAll(timebox.JSONEvent, data)
}

func buildAppendFunctionSQL(spec appendFunctionSpec) string {
	args := []string{
		"p_store TEXT",
		"p_aggregate_key TEXT",
		"p_aggregate_parts TEXT[]",
		"p_expected_sequence BIGINT",
	}
	if spec.status {
		args = append(args,
			"p_status TEXT", "p_status_at BIGINT",
		)
	}
	if spec.labels {
		args = append(args, "p_labels JSONB")
	}
	args = append(args, "p_events TEXT[]")

	decls := []string{
		"v_base_seq BIGINT := 0;",
		"v_snapshot_seq BIGINT := 0;",
		"v_next_seq BIGINT := 0;",
		"v_current_seq BIGINT := 0;",
		"v_event_count BIGINT := " +
			"COALESCE(array_length(p_events, 1), 0);",
	}
	if spec.labels {
		decls = append(decls,
			"v_labels JSONB := '{}'::jsonb;",
			"v_removed_keys TEXT[] := ARRAY[]::TEXT[];",
			"v_set_labels JSONB := '{}'::jsonb;",
		)
	}

	selectExprs := []string{
		"COALESCE(s.base_seq, 0)",
		"COALESCE(s.snapshot_seq, 0)",
	}
	intoVars := []string{"v_base_seq", "v_snapshot_seq"}
	if spec.labels {
		selectExprs = append(selectExprs, "i.labels")
		intoVars = append(intoVars, "v_labels")
	}
	selectExprs = append(selectExprs, `
       COALESCE((
           SELECT e.sequence + 1
           FROM timebox_events e
           WHERE e.store = p_store
             AND e.aggregate_key = p_aggregate_key
           ORDER BY e.sequence DESC
           LIMIT 1
       ), COALESCE(s.base_seq, 0))`)
	intoVars = append(intoVars, "v_next_seq")

	var update strings.Builder
	if spec.labels {
		update.WriteString(`
	SELECT COALESCE(array_agg(kv.key), ARRAY[]::TEXT[])
	INTO v_removed_keys
	FROM jsonb_each_text(p_labels) AS kv
	WHERE kv.value = '';

	SELECT COALESCE(
		jsonb_object_agg(kv.key, to_jsonb(kv.value)),
		'{}'::jsonb
	)
	INTO v_set_labels
	FROM jsonb_each_text(p_labels) AS kv
	WHERE kv.value <> '';
`)
	}
	var assignments []string
	if spec.status {
		assignments = append(assignments,
			"status = p_status",
			"status_at = COALESCE(p_status_at, 0)",
		)
	}
	if spec.labels {
		assignments = append(assignments,
			"labels = (v_labels - v_removed_keys) || v_set_labels",
		)
	}
	if len(assignments) > 0 {
		update.WriteString("UPDATE timebox_index\nSET ")
		update.WriteString(
			strings.Join(assignments, ",\n    "),
		)
		update.WriteString(`
WHERE store = p_store AND aggregate_key = p_aggregate_key;
`)
	}

	return fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s(
	%s
) RETURNS TABLE(
	success BOOLEAN,
	actual_sequence BIGINT,
	new_events TEXT[]
) AS $$
DECLARE
	%s
BEGIN
	IF p_expected_sequence = 0 THEN
		INSERT INTO timebox_index (
			store, aggregate_key, aggregate_parts, labels
		) VALUES (
			p_store, p_aggregate_key,
			p_aggregate_parts, '{}'::jsonb
		)
		ON CONFLICT (store, aggregate_key) DO NOTHING;
	END IF;

	SELECT %s
	INTO %s
	FROM timebox_index i
	LEFT JOIN timebox_snapshot s
	  ON s.store = i.store
	  AND s.aggregate_key = i.aggregate_key
	WHERE i.store = p_store
	  AND i.aggregate_key = p_aggregate_key
	FOR UPDATE OF i;

	IF NOT FOUND THEN
		success := FALSE;
		actual_sequence := 0;
		new_events := ARRAY[]::TEXT[];
		RETURN NEXT;
		RETURN;
	END IF;

	v_current_seq := GREATEST(v_next_seq, v_snapshot_seq);
	IF p_expected_sequence <> v_current_seq THEN
		success := FALSE;
		actual_sequence := v_current_seq;
		SELECT COALESCE(
			array_agg(e.data ORDER BY e.sequence),
			ARRAY[]::TEXT[]
		)
		INTO new_events
		FROM timebox_events e
		WHERE e.store = p_store
		  AND e.aggregate_key = p_aggregate_key
		  AND e.sequence >= p_expected_sequence;
		RETURN NEXT;
		RETURN;
	END IF;

	INSERT INTO timebox_events (
		store, aggregate_key, sequence, data
	)
	SELECT p_store, p_aggregate_key,
		p_expected_sequence + ev.ord - 1, ev.data
	FROM unnest(
		COALESCE(p_events, ARRAY[]::TEXT[])
	) WITH ORDINALITY AS ev(data, ord);
%s
	success := TRUE;
	actual_sequence := v_current_seq + v_event_count;
	new_events := ARRAY[]::TEXT[];
	RETURN NEXT;
END;
$$ LANGUAGE plpgsql
`,
		spec.name,
		strings.Join(args, ",\n\t"),
		strings.Join(decls, "\n\t"),
		strings.Join(selectExprs, ",\n       "),
		strings.Join(intoVars, ", "),
		update.String(),
	)
}

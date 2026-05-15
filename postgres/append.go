package postgres

import (
	"context"
	"fmt"
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
		SELECT success, actual_sequence
		FROM timebox_append_plain(
			$1, $2, $3, $4, $5::bigint[], $6::text[], $7::text[]
		)
	`

	appendStatusQuery = `
		SELECT success, actual_sequence
		FROM timebox_append_status(
			$1, $2, $3, $4, $5, $6,
			$7::bigint[], $8::text[], $9::text[]
		)
	`

	appendLabelsQuery = `
		SELECT success, actual_sequence
		FROM timebox_append_labels(
			$1, $2, $3, $4, $5::text[], $6::text[],
			$7::bigint[], $8::text[], $9::text[]
		)
	`

	appendStatusLabelsQuery = `
		SELECT success, actual_sequence
		FROM timebox_append_status_labels(
			$1, $2, $3, $4, $5, $6, $7::text[], $8::text[],
			$9::bigint[], $10::text[], $11::text[]
		)
	`
)

const checkSequenceQuery = `
	SELECT GREATEST(
		COALESCE((
			SELECT e.sequence + 1
			FROM timebox_events e
			WHERE e.store = $1
			  AND e.aggregate_key = $2
			ORDER BY e.sequence DESC
			LIMIT 1
		), 0),
		COALESCE((
			SELECT s.snapshot_seq
			FROM timebox_snapshots s
			WHERE s.store = $1
			  AND s.aggregate_key = $2
		), 0)
	)
`

// Append appends events if the expected sequence matches
func (p *Persistence) Append(req timebox.AppendRequest) error {
	ctx := context.Background()
	key, parts := aggregateKey(req.ID)
	if len(req.Events) == 0 && req.Status == nil && len(req.Labels) == 0 {
		return p.checkConflict(ctx, req.ID, key, req.ExpectedSequence)
	}
	evAts, evTypes, evData := encodeAppendEvents(req.Events)
	lblKeys, lblVals := encodeLabels(req.Labels)
	var err error

	var status any
	var statusAt int64
	if req.Status != nil {
		status = *req.Status
		statusAt = req.StatusAt.UnixMilli()
	}

	var success bool
	var actualSeq int64

	switch {
	case req.Status != nil && len(req.Labels) > 0:
		err = p.pool.QueryRow(ctx, appendStatusLabelsQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			status, statusAt, lblKeys, lblVals,
			evAts, evTypes, evData,
		).Scan(&success, &actualSeq)
	case req.Status != nil:
		err = p.pool.QueryRow(ctx, appendStatusQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			status, statusAt, evAts, evTypes, evData,
		).Scan(&success, &actualSeq)
	case len(req.Labels) > 0:
		err = p.pool.QueryRow(ctx, appendLabelsQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			lblKeys, lblVals, evAts, evTypes, evData,
		).Scan(&success, &actualSeq)
	default:
		err = p.pool.QueryRow(ctx, appendPlainQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			evAts, evTypes, evData,
		).Scan(&success, &actualSeq)
	}
	if err != nil {
		return err
	}
	if success {
		return nil
	}
	evs, err := p.loadEvents(ctx, req.ID, key, req.ExpectedSequence)
	if err != nil {
		return err
	}
	return &timebox.VersionConflictError{
		ExpectedSequence: req.ExpectedSequence,
		ActualSequence:   actualSeq,
		NewEvents:        evs,
	}
}

func (p *Persistence) checkConflict(
	ctx context.Context, id timebox.AggregateID, key string, expected int64,
) error {
	var actual int64
	if err := p.pool.QueryRow(
		ctx, checkSequenceQuery, p.Prefix, key,
	).Scan(&actual); err != nil {
		return err
	}
	if expected == actual {
		return nil
	}
	var evs []*timebox.Event
	if expected < actual {
		var err error
		evs, err = p.loadEvents(ctx, id, key, expected)
		if err != nil {
			return err
		}
	}
	return &timebox.VersionConflictError{
		ExpectedSequence: expected,
		ActualSequence:   actual,
		NewEvents:        evs,
	}
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
		args = append(args,
			"p_label_keys TEXT[]",
			"p_label_values TEXT[]",
		)
	}
	args = append(args,
		"p_event_ats BIGINT[]",
		"p_event_types TEXT[]",
		"p_event_data TEXT[]",
	)

	decls := []string{
		"v_base_seq BIGINT := 0;",
		"v_snapshot_seq BIGINT := 0;",
		"v_next_seq BIGINT := 0;",
		"v_current_seq BIGINT := 0;",
		"v_event_count BIGINT := " +
			"COALESCE(array_length(p_event_data, 1), 0);",
	}
	selectExprs := []string{
		"COALESCE(s.base_seq, 0)",
		"COALESCE(s.snapshot_seq, 0)",
	}
	intoVars := []string{"v_base_seq", "v_snapshot_seq"}
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
	var assigns []string
	if spec.status {
		assigns = append(assigns,
			"status = p_status",
			"status_at = COALESCE(p_status_at, 0)",
		)
	}
	if len(assigns) != 0 {
		update.WriteString("UPDATE timebox_statuses\nSET ")
		update.WriteString(strings.Join(assigns, ",\n    "))
		update.WriteString(`
WHERE store = p_store AND aggregate_key = p_aggregate_key;
`)
	}
	if spec.labels {
		update.WriteString(`
	DELETE FROM timebox_labels li
	USING unnest(
		COALESCE(p_label_keys, ARRAY[]::TEXT[]),
		COALESCE(p_label_values, ARRAY[]::TEXT[])
	) AS lbl(label, value)
	WHERE li.store = p_store
	  AND li.aggregate_key = p_aggregate_key
	  AND li.label = lbl.label
	  AND lbl.value = '';

	INSERT INTO timebox_labels (
		store, aggregate_key, label, value
	)
	SELECT p_store, p_aggregate_key, lbl.label, lbl.value
	FROM unnest(
		COALESCE(p_label_keys, ARRAY[]::TEXT[]),
		COALESCE(p_label_values, ARRAY[]::TEXT[])
	) AS lbl(label, value)
	WHERE lbl.value <> ''
	ON CONFLICT (store, aggregate_key, label) DO UPDATE
	SET value = EXCLUDED.value;
`)
	}

	return fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s(
	%s
) RETURNS TABLE(
	success BOOLEAN,
	actual_sequence BIGINT
) AS $$
DECLARE
	%s
BEGIN
	IF p_expected_sequence = 0 THEN
		INSERT INTO timebox_statuses (
			store, aggregate_key, aggregate_parts
		) VALUES (
			p_store, p_aggregate_key, p_aggregate_parts
		)
		ON CONFLICT (store, aggregate_key) DO NOTHING;
	END IF;

	SELECT %s
	INTO %s
	FROM timebox_statuses i
	LEFT JOIN timebox_snapshots s
	  ON s.store = i.store
	  AND s.aggregate_key = i.aggregate_key
	WHERE i.store = p_store
	  AND i.aggregate_key = p_aggregate_key
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
		store, aggregate_key, sequence, event_at, event_type, data
	)
	SELECT p_store, p_aggregate_key,
		p_expected_sequence + ev.ord - 1,
		ev.event_at, ev.event_type, ev.data
	FROM unnest(
		COALESCE(p_event_ats, ARRAY[]::BIGINT[]),
		COALESCE(p_event_types, ARRAY[]::TEXT[]),
		COALESCE(p_event_data, ARRAY[]::TEXT[])
	) WITH ORDINALITY AS ev(event_at, event_type, data, ord);
%s
	success := TRUE;
	actual_sequence := v_current_seq + v_event_count;
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

func encodeAppendEvents(evs []*timebox.Event) ([]int64, []string, [][]byte) {
	ats := make([]int64, 0, len(evs))
	types := make([]string, 0, len(evs))
	data := make([][]byte, 0, len(evs))
	for _, ev := range evs {
		ats = append(ats, ev.Timestamp.UnixNano())
		types = append(types, string(ev.Type))
		data = append(data, ev.Data)
	}
	return ats, types, data
}

func encodeLabels(lbls map[string]string) ([]string, []string) {
	keys := make([]string, 0, len(lbls))
	vals := make([]string, 0, len(lbls))
	for k, v := range lbls {
		keys = append(keys, k)
		vals = append(vals, v)
	}
	return keys, vals
}

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
	tags   bool
}

var appendFunctions = []appendFunctionSpec{
	{name: "timebox_append_plain"},
	{name: "timebox_append_status", status: true},
	{name: "timebox_append_tags", tags: true},
	{
		name:   "timebox_append_status_tags",
		status: true,
		tags:   true,
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

	appendTagsQuery = `
		SELECT success, actual_sequence
		FROM timebox_append_tags(
			$1, $2, $3, $4, $5::text[], $6::boolean[],
			$7::bigint[], $8::text[], $9::text[]
		)
	`

	appendStatusTagsQuery = `
		SELECT success, actual_sequence
		FROM timebox_append_status_tags(
			$1, $2, $3, $4, $5, $6, $7::text[], $8::boolean[],
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

// Append appends every request's events if each expected sequence matches
func (p *Persistence) Append(reqs ...timebox.AppendRequest) error {
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, req := range reqs {
		if err := p.appendOne(ctx, tx, req); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (p *Persistence) appendOne(
	ctx context.Context, q querier, req timebox.AppendRequest,
) error {
	key, parts := aggregateKey(req.ID)
	if len(req.Events) == 0 && req.Status == nil && len(req.Tags) == 0 {
		return p.checkConflict(ctx, q, req.ID, key, req.ExpectedSequence)
	}
	evAts, evTypes, evData := encodeAppendEvents(req.Events)
	tags, tagAdds := encodeTags(req.Tags)
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
	case req.Status != nil && len(req.Tags) > 0:
		err = q.QueryRow(ctx, appendStatusTagsQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			status, statusAt, tags, tagAdds,
			evAts, evTypes, evData,
		).Scan(&success, &actualSeq)
	case req.Status != nil:
		err = q.QueryRow(ctx, appendStatusQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			status, statusAt, evAts, evTypes, evData,
		).Scan(&success, &actualSeq)
	case len(req.Tags) > 0:
		err = q.QueryRow(ctx, appendTagsQuery,
			p.Prefix, key, parts, req.ExpectedSequence,
			tags, tagAdds, evAts, evTypes, evData,
		).Scan(&success, &actualSeq)
	default:
		err = q.QueryRow(ctx, appendPlainQuery,
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
	evs, err := p.loadEvents(ctx, q, req.ID, key, req.ExpectedSequence)
	if err != nil {
		return err
	}
	return &timebox.VersionConflictError{
		ID:               req.ID,
		ExpectedSequence: req.ExpectedSequence,
		ActualSequence:   actualSeq,
		NewEvents:        evs,
	}
}

func (p *Persistence) checkConflict(
	ctx context.Context, q querier, id timebox.AggregateID, key string,
	expected int64,
) error {
	var actual int64
	if err := q.QueryRow(
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
		evs, err = p.loadEvents(ctx, q, id, key, expected)
		if err != nil {
			return err
		}
	}
	return &timebox.VersionConflictError{
		ID:               id,
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
	if spec.tags {
		args = append(args,
			"p_tags TEXT[]",
			"p_tag_adds BOOLEAN[]",
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
	if spec.tags {
		update.WriteString(`
	DELETE FROM timebox_tags ti
	USING unnest(
		COALESCE(p_tags, ARRAY[]::TEXT[]),
		COALESCE(p_tag_adds, ARRAY[]::BOOLEAN[])
	) AS item(tag, enabled)
	WHERE ti.store = p_store
	  AND ti.aggregate_key = p_aggregate_key
	  AND ti.tag = item.tag
	  AND NOT item.enabled;

	INSERT INTO timebox_tags (
		store, aggregate_key, tag
	)
	SELECT p_store, p_aggregate_key, item.tag
	FROM unnest(
		COALESCE(p_tags, ARRAY[]::TEXT[]),
		COALESCE(p_tag_adds, ARRAY[]::BOOLEAN[])
	) AS item(tag, enabled)
	WHERE item.enabled
	ON CONFLICT (store, aggregate_key, tag) DO NOTHING;
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

func encodeTags(values map[string]bool) ([]string, []bool) {
	tags := make([]string, 0, len(values))
	adds := make([]bool, 0, len(values))
	for tag, add := range values {
		tags = append(tags, tag)
		adds = append(adds, add)
	}
	return tags, adds
}

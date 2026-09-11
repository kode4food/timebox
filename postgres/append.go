package postgres

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/check"
)

type (
	appendResult struct {
		actualSeq int64
		success   bool
	}

	encodedEvents struct {
		ats   []int64
		types []string
		data  [][]byte
	}
)

const appendQuery = `
	SELECT success, actual_sequence
	FROM timebox_append(
		$1, $2, $3, $4, $5, $6::text[], $7::boolean[],
		$8::bigint[], $9::text[], $10::bytea[]
	)
`

const (
	lockKeyQuery = `
		SELECT 1 FROM timebox_statuses
		WHERE aggregate_key = $1
		FOR UPDATE
	`

	// COLLATE "C" matches the Go sort, so both agree on lock order
	lockKeysQuery = `
		SELECT 1 FROM timebox_statuses
		WHERE aggregate_key = ANY($1)
		ORDER BY aggregate_key COLLATE "C"
		FOR UPDATE
	`
)

// checkSequenceQuery asserts a sequence without mutating, shaped like
// timebox_append so both scan the same way
const checkSequenceQuery = `
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
`
const appendFunctionSQL = `
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
`

// Append appends every request's events if each expected sequence matches
func (b *Backend) Append(reqs ...timebox.AppendRequest) error {
	if err := check.Distinct(reqs); err != nil {
		return err
	}

	ctx := context.Background()
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Lock in a statement of its own: the append function reads the next
	// sequence through a subquery that would otherwise use a stale snapshot
	if err := b.lockAppends(ctx, tx, reqs); err != nil {
		return err
	}
	if err := b.appendAll(ctx, tx, reqs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Lock in key order, but append in request order to report the first
// conflict
func (b *Backend) lockAppends(
	ctx context.Context, tx pgx.Tx, reqs []timebox.AppendRequest,
) error {
	keys := make([]string, 0, len(reqs))
	var inserts map[string][]string
	for _, req := range reqs {
		key, parts := aggregateKey(req.ID)
		keys = append(keys, key)
		if req.ExpectedSequence == 0 && check.Mutates(req) {
			if inserts == nil {
				inserts = map[string][]string{}
			}
			inserts[key] = parts
		}
	}
	slices.Sort(keys)
	for _, key := range keys {
		if parts, ok := inserts[key]; ok {
			if err := b.insertAggregate(ctx, tx, key, parts); err != nil {
				return err
			}
		}
	}

	q, arg := lockKeysQuery, any(keys)
	if len(keys) == 1 {
		q, arg = lockKeyQuery, any(keys[0])
	}
	_, err := tx.Exec(ctx, q, arg)
	return err
}

func (b *Backend) appendAll(
	ctx context.Context, tx pgx.Tx, reqs []timebox.AppendRequest,
) error {
	batch := &pgx.Batch{}
	for _, req := range reqs {
		q, args := appendCall(req)
		batch.Queue(q, args...)
	}
	br := tx.SendBatch(ctx, batch)

	failed := -1
	var failedSeq int64
	var scanErr error
	for i := range reqs {
		res, err := scanAppendResult(br.QueryRow())
		if err != nil && scanErr == nil {
			scanErr = err
		}
		if !res.success && failed < 0 {
			failed, failedSeq = i, res.actualSeq
		}
	}
	if err := br.Close(); err != nil {
		return err
	}
	if scanErr != nil {
		return scanErr
	}
	if failed < 0 {
		return nil
	}
	return b.versionConflict(ctx, tx, reqs[failed], failedSeq)
}

func (b *Backend) versionConflict(
	ctx context.Context, q querier, req timebox.AppendRequest, actual int64,
) error {
	var evs []*timebox.Event
	if req.ExpectedSequence < actual {
		key, _ := aggregateKey(req.ID)
		var err error
		evs, err = b.loadEvents(ctx, q, eventRange{
			id:      req.ID,
			key:     key,
			fromSeq: req.ExpectedSequence,
		})
		if err != nil {
			return err
		}
	}
	return &timebox.VersionConflictError{
		ID:               req.ID,
		ExpectedSequence: req.ExpectedSequence,
		ActualSequence:   actual,
		NewEvents:        evs,
	}
}

// appendCall builds the query and arguments one request needs
func appendCall(req timebox.AppendRequest) (string, []any) {
	key, parts := aggregateKey(req.ID)
	if !check.Mutates(req) {
		return checkSequenceQuery, []any{key, req.ExpectedSequence}
	}
	evs := encodeAppendEvents(req.Events)
	tags, tagAdds := encodeTags(req.Tags)

	var status any
	var statusAt int64
	if req.Status != nil {
		status = *req.Status
		statusAt = req.StatusAt.UnixMilli()
	}

	return appendQuery, []any{
		key, parts, req.ExpectedSequence,
		status, statusAt, tags, tagAdds,
		evs.ats, evs.types, evs.data,
	}
}

func scanAppendResult(row pgx.Row) (appendResult, error) {
	var res appendResult
	err := row.Scan(&res.success, &res.actualSeq)
	return res, err
}

func encodeAppendEvents(evs []*timebox.Event) encodedEvents {
	res := encodedEvents{
		ats:   make([]int64, 0, len(evs)),
		types: make([]string, 0, len(evs)),
		data:  make([][]byte, 0, len(evs)),
	}
	for _, ev := range evs {
		res.ats = append(res.ats, ev.Timestamp.UnixNano())
		res.types = append(res.types, string(ev.Type))
		res.data = append(res.data, ev.Data)
	}
	return res
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

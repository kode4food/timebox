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

// Lock in key order, but append in request order to report the first conflict
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

	q, arg := sqlLockKeys, any(keys)
	if len(keys) == 1 {
		q, arg = sqlLockKey, any(keys[0])
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
		return sqlCheckSequence, []any{key, req.ExpectedSequence}
	}
	evs := encodeAppendEvents(req.Events)
	tags, tagAdds := encodeTags(req.Tags)

	var status any
	var statusAt int64
	if req.Status != nil {
		status = *req.Status
		statusAt = req.StatusAt.UnixMilli()
	}

	return sqlAppend, []any{
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

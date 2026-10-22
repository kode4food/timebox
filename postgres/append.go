package postgres

import (
	"context"
	"errors"
	"slices"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/check"
)

type encodedEvents struct {
	ats   []int64
	types []string
	data  [][]byte
}

const conflictCode = "TB409"

// Append appends every request's events if each expected sequence matches
func (b *Backend) Append(reqs ...timebox.AppendRequest) error {
	if err := check.Distinct(reqs); err != nil {
		return err
	}

	ctx := context.Background()
	if !slices.ContainsFunc(reqs, check.Mutates) {
		return b.checkSequences(ctx, reqs)
	}

	// A batch runs as one implicit transaction, so the append stays atomic
	batch := &pgx.Batch{}

	// Lock in statements of their own: the append function reads the next
	// sequence through a subquery that would otherwise use a stale snapshot
	queueLocks(batch, reqs)
	first := batch.Len()
	for _, req := range reqs {
		q, args := appendCall(req)
		batch.Queue(q, args...)
	}
	for _, req := range reqs {
		if !req.Delete {
			continue
		}
		queueDelete(batch, req)
	}
	if err := b.sendAppend(ctx, batch, reqs, first); err != nil {
		return err
	}
	b.publisher.PublishAppends(reqs...)
	return nil
}

func (b *Backend) checkSequences(
	ctx context.Context, reqs []timebox.AppendRequest,
) error {
	keys := make([]string, 0, len(reqs))
	for _, req := range reqs {
		key, _ := aggregateKey(req.ID)
		keys = append(keys, key)
	}
	rows, err := b.pool.Query(ctx, sqlCheckSequences, keys)
	if err != nil {
		return err
	}
	seqs, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return err
	}
	for i, req := range reqs {
		if seqs[i] == req.ExpectedSequence {
			continue
		}
		return b.versionConflict(ctx, b.pool, req, seqs[i])
	}
	return nil
}

// sendAppend expects the request statements to start at batch index first
func (b *Backend) sendAppend(
	ctx context.Context, batch *pgx.Batch, reqs []timebox.AppendRequest,
	first int,
) error {
	br := b.pool.SendBatch(ctx, batch)
	var failed int
	var err error
	for i := range batch.Len() {
		if _, err = br.Exec(); err != nil {
			failed = i - first
			break
		}
	}
	closeErr := br.Close()
	if err == nil {
		return closeErr
	}
	actual, ok := parseConflict(err)
	if !ok {
		return err
	}
	return b.versionConflict(ctx, b.pool, reqs[failed], actual)
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

// Lock in key order, but append in request order to report the first conflict.
// A check-only request takes a shared lock, so checks never wait on each other
func queueLocks(batch *pgx.Batch, reqs []timebox.AppendRequest) {
	keys := make([]string, 0, len(reqs))
	var inserts map[string][]string
	var shared map[string]bool
	for _, req := range reqs {
		key, parts := aggregateKey(req.ID)
		keys = append(keys, key)
		switch {
		case !check.Mutates(req):
			if shared == nil {
				shared = map[string]bool{}
			}
			shared[key] = true
		case req.ExpectedSequence == 0 && !req.Delete:
			if inserts == nil {
				inserts = map[string][]string{}
			}
			inserts[key] = parts
		}
	}
	slices.Sort(keys)
	for _, key := range keys {
		if parts, ok := inserts[key]; ok {
			batch.Queue(sqlInsertAggregate, key, parts)
		}
	}

	if shared == nil && len(keys) == 1 {
		batch.Queue(sqlLockKey, keys[0])
		return
	}
	if shared == nil {
		batch.Queue(sqlLockKeys, keys)
		return
	}

	// One statement per key keeps the key order while mixing lock strengths
	for _, key := range keys {
		q := sqlLockKey
		if shared[key] {
			q = sqlShareKey
		}
		batch.Queue(q, key)
	}
}

// queueDelete clears an aggregate's records, keeping its sequence
func queueDelete(batch *pgx.Batch, req timebox.AppendRequest) {
	key, _ := aggregateKey(req.ID)
	seq := req.ExpectedSequence + int64(len(req.Events))
	batch.Queue(sqlDeleteEvents, key)
	batch.Queue(sqlDeleteTags, key)
	batch.Queue(sqlMarkDeleted, key)
	batch.Queue(sqlPutSnapshot, key, seq, seq, []byte{})
}

// appendCall builds the query and arguments one request needs
func appendCall(req timebox.AppendRequest) (string, []any) {
	key, parts := aggregateKey(req.ID)
	if req.Delete || !check.Mutates(req) {
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

func parseConflict(err error) (int64, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != conflictCode {
		return 0, false
	}
	seq, err := strconv.ParseInt(pgErr.Detail, 10, 64)
	return seq, err == nil
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

package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
)

// Archive atomically bundles id's snapshot and remaining events into the
// shared archive queue, then removes id's live records
func (b *Backend) Archive(id timebox.AggregateID) error {
	ctx := context.Background()
	key, _ := aggregateKey(id)
	queueKey, queueParts := aggregateKey(timebox.ArchiveQueueID)

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := b.insertAggregate(ctx, tx, queueKey, queueParts); err != nil {
		return err
	}
	keys := []string{key, queueKey}
	if _, err := tx.Exec(ctx, sqlLockKeys, keys); err != nil {
		return err
	}

	record, err := b.bundleArchiveTx(ctx, tx, id, key)
	if err != nil || record == nil {
		return err
	}

	queueSnap, _, err := b.loadSnapshotState(ctx, tx, queueKey)
	if err != nil {
		return err
	}

	ev, err := timebox.NewArchiveQueueEvent(queueSnap.nextSeq, id, record)
	if err != nil {
		return err
	}
	err = b.appendArchiveEventTx(ctx, tx, queueSnap.nextSeq, ev)
	if err != nil {
		return err
	}

	if err := b.deleteAggregateTx(ctx, tx, key); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (b *Backend) bundleArchiveTx(
	ctx context.Context, tx pgx.Tx, id timebox.AggregateID, key string,
) (*timebox.ArchiveRecord, error) {
	state, found, err := b.loadSnapshotState(ctx, tx, key)
	if err != nil || !found {
		return nil, err
	}

	var snapData []byte
	row := tx.QueryRow(ctx, sqlGetSnapshot, key)
	err = row.Scan(&snapData, &state.snapSeq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	events, err := b.loadEvents(ctx, tx, eventRange{
		id: id, key: key, fromSeq: state.baseSeq,
	})
	if err != nil {
		return nil, err
	}

	return &timebox.ArchiveRecord{
		AggregateID:      id,
		SnapshotData:     snapData,
		SnapshotSequence: state.snapSeq,
		Events:           events,
	}, nil
}

func (b *Backend) appendArchiveEventTx(
	ctx context.Context, tx pgx.Tx, atSeq int64, ev *timebox.Event,
) error {
	req := timebox.AppendRequest{
		ID:               timebox.ArchiveQueueID,
		ExpectedSequence: atSeq,
		Events:           []*timebox.Event{ev},
	}
	q, args := appendCall(req)
	_, err := tx.Exec(ctx, q, args...)
	if actual, ok := parseConflict(err); ok {
		// The raise aborted tx, so the conflict's events load outside it
		return b.versionConflict(ctx, b.pool, req, actual)
	}
	return err
}

func (b *Backend) deleteAggregateTx(
	ctx context.Context, tx pgx.Tx, key string,
) error {
	batch := &pgx.Batch{}
	stmts := []string{
		sqlDeleteEvents, sqlDeleteSnapshot, sqlDeleteTags, sqlDeleteStatus,
	}
	for _, q := range stmts {
		batch.Queue(q, key)
	}
	return tx.SendBatch(ctx, batch).Close()
}

package redis

import (
	"context"
	"errors"
	"time"

	"github.com/kode4food/timebox"
)

// Archive atomically bundles id's snapshot and remaining events into the shared
// archive queue, then removes id's live records
func (b *Backend) Archive(id timebox.AggregateID) error {
	snapKey := b.buildKey(id, snapshotValSuffix)
	snapSeqKey := b.buildKey(id, snapshotSeqSuffix)
	eventsKey := b.buildKey(id, eventsSuffix)
	statusKey := b.buildStatusHashKey()
	tagStateKey := b.buildTagStateKey(id)
	tagRootKey := b.buildTagRootKey()
	archiveEventsKey := b.buildKey(timebox.ArchiveQueueID, eventsSuffix)
	archiveSeqKey := b.buildKey(timebox.ArchiveQueueID, snapshotSeqSuffix)

	keys := []string{
		snapKey, snapSeqKey, eventsKey, statusKey, tagStateKey, tagRootKey,
		archiveEventsKey, archiveSeqKey,
	}
	args := []any{
		joinAggregateID(id),
		string(id.Type),
		string(id.Key),
		time.Now().UTC().Format(time.RFC3339Nano),
		string(timebox.ArchiveQueueID.Type),
		string(timebox.ArchiveQueueID.Key),
	}

	result, err := b.archive.Run(
		context.Background(), b.client, keys, args...,
	).Result()
	if err != nil {
		return err
	}

	res, ok := result.([]any)
	if !ok || len(res) == 0 {
		return errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}
	return nil
}

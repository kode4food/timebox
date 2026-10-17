package raft

import (
	"context"

	"github.com/kode4food/timebox"
)

// Archive archives an aggregate and removes it from active storage
func (b *Backend) Archive(id timebox.AggregateID) error {
	propID := b.newProposalID()
	_, err := b.applyWithTimeout(
		context.Background(),
		MakeArchiveCommand(propID, &ArchiveCommand{ID: id}),
		propID,
		nil,
	)
	return err
}

func deleteAggregateTx(
	b *kvBucket, encodedID string, meta *AggregateMeta,
) error {
	if meta.Status != "" {
		key := statusIndexKey(meta.Status, encodedID, meta.StatusAt)
		if err := b.Delete(key); err != nil {
			return err
		}
	}
	for tag := range meta.Tags {
		if err := b.Delete(tagIndexKey(tag, encodedID)); err != nil {
			return err
		}
	}

	// A cursor in the batched apply transaction re-sorts its pending writes,
	// so delete the event keys the meta already bounds
	pfx := aggregateEventPrefix(encodedID)
	for seq := meta.BaseSequence; seq < meta.CurrentSequence; seq++ {
		key := aggregateEventKeyFromPrefix(pfx, seq)
		if err := b.Delete(key); err != nil {
			return err
		}
	}
	if err := b.Delete(aggregateSnapshotKey(encodedID)); err != nil {
		return err
	}
	return b.Delete(AggregateMetaKey(encodedID))
}

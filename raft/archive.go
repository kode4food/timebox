package raft

import (
	"bytes"
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
		if err := b.Delete(statusIndexKey(meta.Status, encodedID)); err != nil {
			return err
		}
	}
	for tag := range meta.Tags {
		if err := b.Delete(tagIndexKey(tag, encodedID)); err != nil {
			return err
		}
	}

	pfx := aggregateEventPrefix(encodedID)
	keys, err := collectKeysTx(b, pfx)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	if err := b.Delete(aggregateSnapshotKey(encodedID)); err != nil {
		return err
	}
	return b.Delete(AggregateMetaKey(encodedID))
}

func collectKeysTx(b *kvBucket, pfx []byte) ([][]byte, error) {
	c := b.Cursor()
	defer func() { _ = c.Close() }()

	var keys [][]byte
	for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
		keys = append(keys, append([]byte(nil), k...))
		k, _ = c.Next()
	}
	return keys, nil
}

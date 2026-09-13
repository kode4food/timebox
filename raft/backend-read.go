package raft

import (
	"bytes"
	"strings"

	"github.com/kode4food/timebox"
)

// LoadEvents loads events for one aggregate starting at the requested sequence
func (b *Backend) LoadEvents(
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	var res *timebox.EventsResult

	encodedID := encodeAggregateID(req.ID)
	err := b.db.View(func(tx *kvTx) error {
		meta, ok, err := loadMetaTx(tx.Bucket(bucketName), encodedID)
		if err != nil {
			return err
		}
		if !ok {
			res = &timebox.EventsResult{
				StartSequence: req.FromSeq,
				Events:        []*timebox.Event{},
			}
			return nil
		}

		startSeq := max(req.FromSeq, meta.BaseSequence)

		evs, err := loadEventsTx(tx.Bucket(bucketName), encodedID, startSeq)
		if err != nil {
			return err
		}
		res = &timebox.EventsResult{
			StartSequence: startSeq,
			Events:        evs,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// LoadSnapshot returns the latest snapshot and tail events for one aggregate
func (b *Backend) LoadSnapshot(
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	var rec *timebox.SnapshotRecord

	encodedID := encodeAggregateID(req.ID)
	err := b.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		meta, ok, err := loadMetaTx(b, encodedID)
		if err != nil {
			return err
		}
		if !ok {
			rec = &timebox.SnapshotRecord{}
			return nil
		}

		startSeq := max(meta.SnapshotSequence, meta.BaseSequence)

		evs, err := loadEventsTx(b, encodedID, startSeq)
		if err != nil {
			return err
		}
		rec = &timebox.SnapshotRecord{
			Data:     append([]byte(nil), b.Get(aggregateSnapshotKey(encodedID))...),
			Sequence: meta.SnapshotSequence,
			Events:   evs,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// ListAggregates lists known aggregate IDs of the given type, or of every type
// when it is empty
func (b *Backend) ListAggregates(
	typ timebox.ID,
) ([]timebox.AggregateID, error) {
	var ids []timebox.AggregateID

	err := b.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		defer func() { _ = c.Close() }()
		pfx := AggregateMetaPrefix()
		if typ != "" {
			pfx = append(pfx, encodeAggregateType(typ)...)
		}
		for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			key := string(k)
			if !strings.HasSuffix(key, metaSuffix) {
				k, _ = c.Next()
				continue
			}
			enc := strings.TrimPrefix(key, aggRootPrefix)
			enc = strings.TrimSuffix(enc, metaSuffix)
			nextID, err := decodeAggregateID(enc)
			if err != nil {
				return err
			}
			ids = append(ids, nextID)
			k, _ = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (b *Backend) checkConflict(
	id timebox.AggregateID, expected int64,
) error {
	encodedID := encodeAggregateID(id)
	return b.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		meta, err := loadOrCreateMetaTx(b, encodedID)
		if err != nil {
			return err
		}
		if expected == meta.CurrentSequence {
			return nil
		}
		conflict := &timebox.VersionConflictError{
			ID:               id,
			ExpectedSequence: expected,
			ActualSequence:   meta.CurrentSequence,
		}
		if expected < meta.CurrentSequence {
			startSeq := max(expected, meta.BaseSequence)
			evs, err := loadEventsTx(b, encodedID, startSeq)
			if err != nil {
				return err
			}
			conflict.NewEvents = evs
		}
		return conflict
	})
}

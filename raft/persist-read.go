package raft

import (
	"bytes"
	"strings"

	"github.com/kode4food/timebox"
)

// LoadEvents loads events for one aggregate starting at the requested sequence
func (p *Persistence) LoadEvents(
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	var res *timebox.EventsResult

	encodedID := encodeAggregateID(req.ID)
	err := p.db.View(func(tx *kvTx) error {
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
func (p *Persistence) LoadSnapshot(
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	var rec *timebox.SnapshotRecord

	encodedID := encodeAggregateID(req.ID)
	err := p.db.View(func(tx *kvTx) error {
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

// ListAggregates lists known aggregate IDs that share the given prefix
func (p *Persistence) ListAggregates(
	id timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	var ids []timebox.AggregateID

	err := p.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		defer func() { _ = c.Close() }()
		pfx := AggregateMetaPrefix()
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
			if nextID.HasPrefix(id) {
				ids = append(ids, nextID)
			}
			k, _ = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (p *Persistence) checkConflict(
	id timebox.AggregateID, expected int64,
) error {
	encodedID := encodeAggregateID(id)
	var conflict error
	err := p.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		meta, ok, err := loadMetaTx(b, encodedID)
		if err != nil || !ok {
			return err
		}
		if expected == meta.CurrentSequence {
			return nil
		}
		vc := &timebox.VersionConflictError{
			ExpectedSequence: expected,
			ActualSequence:   meta.CurrentSequence,
		}
		if expected < meta.CurrentSequence {
			startSeq := max(expected, meta.BaseSequence)
			evs, err := loadEventsTx(b, encodedID, startSeq)
			if err != nil {
				return err
			}
			vc.NewEvents = evs
		}
		conflict = vc
		return nil
	})
	if err != nil {
		return err
	}
	return conflict
}

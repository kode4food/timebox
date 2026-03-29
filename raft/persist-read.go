package raft

import (
	"bytes"
	"strings"

	"go.etcd.io/bbolt"

	"github.com/kode4food/timebox"
)

func (p *Persistence) LoadEvents(
	id timebox.AggregateID, fromSeq int64,
) (*timebox.EventsResult, error) {
	var res *timebox.EventsResult

	encodedID := encodeAggregateID(id)
	err := p.db.View(func(tx *bbolt.Tx) error {
		meta, ok, err := loadMetaTx(tx.Bucket(bucketName), encodedID)
		if err != nil {
			return err
		}
		if !ok {
			res = &timebox.EventsResult{
				StartSequence: fromSeq,
				Events:        []*timebox.Event{},
			}
			return nil
		}

		startSeq := max(fromSeq, meta.BaseSequence)

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

func (p *Persistence) LoadSnapshot(
	id timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	var rec *timebox.SnapshotRecord

	encodedID := encodeAggregateID(id)
	err := p.db.View(func(tx *bbolt.Tx) error {
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

func (p *Persistence) ListAggregates(
	id timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	var ids []timebox.AggregateID

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
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
	err := p.db.View(func(tx *bbolt.Tx) error {
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

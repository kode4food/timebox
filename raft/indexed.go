package raft

import (
	"bytes"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kode4food/timebox"
)

// GetAggregateStatus returns the current derived status for one aggregate
func (b *Backend) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	var status string
	err := b.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		meta, ok, err := loadMetaTx(b, encodeAggregateID(id))
		if err != nil {
			return err
		}
		if ok {
			status = meta.Status
		}
		return nil
	})
	return status, err
}

// ListAggregatesByStatus lists aggregates matching the query, ordered by
// status time. Type narrows the status-index scan; KeyPrefix and Through
// filter each entry
func (b *Backend) ListAggregatesByStatus(
	q timebox.StatusQuery,
) ([]timebox.StatusEntry, error) {
	bound := int64(math.MaxInt64)
	if !q.Through.IsZero() {
		bound = q.Through.UnixMilli()
	}

	var res []timebox.StatusEntry
	err := b.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		defer func() { _ = c.Close() }()
		pfx := statusIndexPrefix(q.Status)
		if q.Type != "" {
			pfx = append(pfx, []byte(encodeAggregateType(q.Type))...)
		}
		for k, v := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			ts, err := decodeOptionalInt64(v)
			if err != nil {
				return err
			}
			parts := strings.Split(string(k), "/")
			id, err := decodeAggregateID(parts[len(parts)-1])
			if err != nil {
				return err
			}
			if ts <= bound &&
				strings.HasPrefix(string(id.Key), string(q.KeyPrefix)) {
				res = append(res, timebox.StatusEntry{
					ID:        id,
					Timestamp: time.UnixMilli(ts).UTC(),
				})
			}
			k, v = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Timestamp.Before(res[j].Timestamp)
	})
	return res, nil
}

// ListAggregatesByTag lists aggregates currently indexed by tag
func (b *Backend) ListAggregatesByTag(
	tag string,
) ([]timebox.AggregateID, error) {
	var ids []timebox.AggregateID

	err := b.db.View(func(tx *kvTx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		defer func() { _ = c.Close() }()
		pfx := tagIndexPrefix(tag)
		for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			parts := strings.Split(string(k), "/")
			id, err := decodeAggregateID(parts[len(parts)-1])
			if err != nil {
				return err
			}
			ids = append(ids, id)
			k, _ = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

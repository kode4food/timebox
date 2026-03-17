package raft

import (
	"bytes"
	"sort"
	"strings"
	"time"

	"go.etcd.io/bbolt"

	"github.com/kode4food/timebox"
)

// GetAggregateStatus returns the current derived status for one aggregate
func (p *Persistence) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	var status string
	err := p.db.View(func(tx *bbolt.Tx) error {
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

// ListAggregatesByStatus lists aggregates currently indexed by status
func (p *Persistence) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	var res []timebox.StatusEntry

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := statusIndexPrefix(status)
		for k, v := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			parts := strings.Split(string(k), "/")
			id, err := decodeAggregateID(parts[len(parts)-1])
			if err != nil {
				return err
			}
			ts, err := decodeInt64(v)
			if err != nil {
				return err
			}
			res = append(res, timebox.StatusEntry{
				ID:        id,
				Timestamp: time.UnixMilli(ts).UTC(),
			})
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

// ListAggregatesByLabel lists aggregates currently indexed by label/value
func (p *Persistence) ListAggregatesByLabel(
	label, value string,
) ([]timebox.AggregateID, error) {
	var ids []timebox.AggregateID

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := labelIndexPrefix(label, value)
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

// ListLabelValues lists the current indexed values for one label
func (p *Persistence) ListLabelValues(label string) ([]string, error) {
	var vals []string

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := labelValuesPrefix(label)
		for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			parts := strings.Split(string(k), "/")
			value, err := decodeKeyPart(parts[len(parts)-1])
			if err != nil {
				return err
			}
			vals = append(vals, value)
			k, _ = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(vals)
	return vals, nil
}

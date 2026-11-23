package raft

import (
	"bytes"
	"maps"
	"slices"
	"time"

	"github.com/kode4food/timebox"
)

// GetAggregateStatus returns the current derived status for one aggregate
func (b *Backend) GetAggregateStatus(id timebox.AggregateID) (string, error) {
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

// ListAggregatesByStatus lists one page of aggregates matching the query, newest first with ties
// broken by aggregate key. It walks the type's status keys backward from Through or After
func (b *Backend) ListAggregatesByStatus(q timebox.StatusQuery) ([]timebox.StatusEntry, error) {
	if q.Limit <= 0 {
		return nil, timebox.ErrInvalidLimit
	}
	var res []timebox.StatusEntry
	err := b.db.View(func(tx *kvTx) error {
		base := statusIndexPrefix(q.Status)
		pfx := append(base, encodeAggregateType(q.Type)...)
		c := tx.Bucket(bucketName).ReverseCursor()
		defer func() { _ = c.Close() }()

		k, v := c.Seek(statusStart(pfx, q))
		for ; k != nil && bytes.HasPrefix(k, pfx) && len(res) < q.Limit; k, v = c.Next() {
			if q.After != nil && bytes.Equal(k, statusPosition(pfx, *q.After)) {
				continue
			}
			ts, err := decodeOptionalInt64(v)
			if err != nil {
				return err
			}
			id, err := decodeStatusIndexID(string(k[len(base):]))
			if err != nil {
				return err
			}
			res = append(res, timebox.StatusEntry{
				ID:        id,
				Timestamp: time.UnixMilli(ts).UTC(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetAggregateTags loads the tags each aggregate currently holds, in one read transaction
func (b *Backend) GetAggregateTags(
	ids ...timebox.AggregateID,
) (map[timebox.AggregateID][]string, error) {
	res := map[timebox.AggregateID][]string{}
	err := b.db.View(func(tx *kvTx) error {
		bkt := tx.Bucket(bucketName)
		for _, id := range ids {
			meta, ok, err := loadMetaTx(bkt, encodeAggregateID(id))
			if err != nil {
				return err
			}
			if ok && len(meta.Tags) > 0 {
				res[id] = slices.Collect(maps.Keys(meta.Tags))
			}
		}
		return nil
	})
	return res, err
}

// statusStart is where a backward walk begins: After's own key, the end of Through's millisecond,
// or the end of the type's keys
func statusStart(pfx []byte, q timebox.StatusQuery) []byte {
	if q.After != nil {
		return statusPosition(pfx, *q.After)
	}
	end := string(pfx)
	if !q.Through.IsZero() {
		end += statusTime(q.Through.UnixMilli()) + "/"
	}
	return append([]byte(end), 0xff)
}

// statusPosition is the index key an entry of pfx's status and type occupies
func statusPosition(pfx []byte, e timebox.StatusEntry) []byte {
	return []byte(string(pfx) + statusTime(e.Timestamp.UnixMilli()) + "/" +
		encodeKeyPart(string(e.ID.Key)))
}

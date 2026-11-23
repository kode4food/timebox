package postgres

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
)

// GetAggregateStatus gets the current status for an aggregate
func (b *Backend) GetAggregateStatus(id timebox.AggregateID) (string, error) {
	ctx := context.Background()
	key, _ := aggregateKey(id)

	var status string
	err := b.pool.QueryRow(ctx, sqlGetStatus, key).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return status, err
}

// ListAggregatesByStatus lists one page of aggregates matching the query, newest first with ties
// broken by aggregate key. Through and After both become an exclusive (time, key) upper bound
func (b *Backend) ListAggregatesByStatus(q timebox.StatusQuery) ([]timebox.StatusEntry, error) {
	if q.Limit <= 0 {
		return nil, timebox.ErrInvalidLimit
	}
	var beforeAt int64 = math.MaxInt64
	var beforeKey string
	if !q.Through.IsZero() {
		beforeAt = q.Through.UnixMilli() + 1
	}
	if q.After != nil {
		beforeAt = q.After.Timestamp.UnixMilli()
		beforeKey, _ = aggregateKey(q.After.ID)
	}
	rows, err := b.pool.Query(
		context.Background(), sqlListByStatus, q.Status, q.Type, beforeAt, beforeKey, q.Limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []timebox.StatusEntry
	for rows.Next() {
		var parts []string
		var ts int64
		if err := rows.Scan(&parts, &ts); err != nil {
			return nil, err
		}
		aggID, err := aggregateID(parts)
		if err != nil {
			return nil, err
		}
		res = append(res, timebox.StatusEntry{
			ID:        aggID,
			Timestamp: time.UnixMilli(ts).UTC(),
		})
	}
	return res, rows.Err()
}

// GetAggregateTags loads the tags each aggregate currently holds, in one query
func (b *Backend) GetAggregateTags(
	ids ...timebox.AggregateID,
) (map[timebox.AggregateID][]string, error) {
	byKey := make(map[string]timebox.AggregateID, len(ids))
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		key, _ := aggregateKey(id)
		byKey[key] = id
		keys = append(keys, key)
	}
	rows, err := b.pool.Query(context.Background(), sqlGetTags, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := map[timebox.AggregateID][]string{}
	for rows.Next() {
		var key, tag string
		if err := rows.Scan(&key, &tag); err != nil {
			return nil, err
		}
		res[byKey[key]] = append(res[byKey[key]], tag)
	}
	return res, rows.Err()
}

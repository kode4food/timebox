package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
)

// GetAggregateStatus gets the current status for an aggregate
func (b *Backend) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	ctx := context.Background()
	key, _ := aggregateKey(id)

	var status string
	err := b.pool.QueryRow(ctx, `
		SELECT status
		FROM timebox_statuses
		WHERE store = $1 AND aggregate_key = $2
	`, b.cfg.Prefix, key).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return status, err
}

// ListAggregatesByStatus lists aggregates for the given status
func (b *Backend) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	rows, err := b.pool.Query(context.Background(), `
		SELECT aggregate_parts, status_at
		FROM timebox_statuses
		WHERE store = $1 AND status = $2
		ORDER BY status_at
	`, b.cfg.Prefix, status)
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

// ListAggregatesByTag lists aggregates for a tag
func (b *Backend) ListAggregatesByTag(
	tag string,
) ([]timebox.AggregateID, error) {
	rows, err := b.pool.Query(context.Background(), `
		SELECT i.aggregate_parts
		FROM timebox_tags ti
		JOIN timebox_statuses i
		  ON i.store = ti.store
		  AND i.aggregate_key = ti.aggregate_key
		WHERE ti.store = $1
		  AND ti.tag = $2
	`, b.cfg.Prefix, tag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []timebox.AggregateID
	for rows.Next() {
		var parts []string
		if err := rows.Scan(&parts); err != nil {
			return nil, err
		}
		aggID, err := aggregateID(parts)
		if err != nil {
			return nil, err
		}
		res = append(res, aggID)
	}
	return res, rows.Err()
}

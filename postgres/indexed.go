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
		WHERE aggregate_key = $1
	`, key).Scan(&status)
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
		WHERE status = $1
		ORDER BY status_at
	`, status)
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
		  ON i.aggregate_key = ti.aggregate_key
		WHERE ti.tag = $1
	`, tag)
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

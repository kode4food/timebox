package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
)

// GetAggregateStatus gets the current status for an aggregate
func (p *Persistence) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	ctx := context.Background()
	key, _ := aggregateKey(id)

	var status string
	err := p.pool.QueryRow(ctx, `
		SELECT status
		FROM timebox_statuses
		WHERE store = $1 AND aggregate_key = $2
	`, p.Prefix, key).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return status, err
}

// ListAggregatesByStatus lists aggregates for the given status
func (p *Persistence) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	rows, err := p.pool.Query(context.Background(), `
		SELECT aggregate_parts, status_at
		FROM timebox_statuses
		WHERE store = $1 AND status = $2
		ORDER BY status_at
	`, p.Prefix, status)
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
		res = append(res, timebox.StatusEntry{
			ID:        aggregateID(parts),
			Timestamp: time.UnixMilli(ts).UTC(),
		})
	}
	return res, rows.Err()
}

// ListAggregatesByLabel lists aggregates for a label/value pair
func (p *Persistence) ListAggregatesByLabel(
	label, value string,
) ([]timebox.AggregateID, error) {
	rows, err := p.pool.Query(context.Background(), `
		SELECT i.aggregate_parts
		FROM timebox_labels li
		JOIN timebox_statuses i
		  ON i.store = li.store
		  AND i.aggregate_key = li.aggregate_key
		WHERE li.store = $1
		  AND li.label = $2
		  AND li.value = $3
	`, p.Prefix, label, value)
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
		res = append(res, aggregateID(parts))
	}
	return res, rows.Err()
}

// ListLabelValues lists values currently used for a label
func (p *Persistence) ListLabelValues(label string) ([]string, error) {
	rows, err := p.pool.Query(context.Background(), `
		SELECT DISTINCT value
		FROM timebox_labels
		WHERE store = $1 AND label = $2
		ORDER BY value
	`, p.Prefix, label)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, rows.Err()
}

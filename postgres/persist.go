package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kode4food/timebox"
)

// Persistence implements timebox.Persistence using Postgres
type Persistence struct {
	timebox.AlwaysReady
	Config
	pool *pgxpool.Pool
}

const connectTimeout = 5 * time.Second

var _ timebox.Backend = (*Persistence)(nil)

// NewStore creates a Store backed by Postgres persistence
func NewStore(cfgs ...Config) (*timebox.Store, error) {
	p, err := NewPersistence(cfgs...)
	if err != nil {
		return nil, err
	}
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	return timebox.NewStore(p, cfg.Timebox)
}

// NewPersistence creates Postgres-backed Persistence
func NewPersistence(cfgs ...Config) (*Persistence, error) {
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newPersistence(cfg)
}

func newPersistence(cfg Config) (*Persistence, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), connectTimeout,
	)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.ConnConfig.DefaultQueryExecMode =
		pgx.QueryExecModeCacheStatement

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	schemaCtx, cancel := context.WithTimeout(
		context.Background(), schemaTimeout,
	)
	defer cancel()

	if err := initSchema(schemaCtx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return &Persistence{
		Config: cfg,
		pool:   pool,
	}, nil
}

// Close closes the Postgres connection pool
func (p *Persistence) Close() error {
	p.pool.Close()
	return nil
}

// LoadEvents loads events starting at fromSeq
func (p *Persistence) LoadEvents(
	id timebox.AggregateID, fromSeq int64,
) (*timebox.EventsResult, error) {
	ctx := context.Background()
	key, _, err := aggregateKey(id)
	if err != nil {
		return nil, err
	}

	var baseSeq int64
	err = p.pool.QueryRow(ctx, `
		SELECT base_seq
		FROM timebox_snapshot
		WHERE store = $1 AND aggregate_key = $2
	`, p.Prefix, key).Scan(&baseSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		baseSeq = 0
	} else if err != nil {
		return nil, err
	}

	start := max(fromSeq, baseSeq)
	evs, err := p.loadEvents(ctx, key, start)
	if err != nil {
		return nil, err
	}
	return &timebox.EventsResult{
		StartSequence: start,
		Events:        evs,
	}, nil
}

// LoadSnapshot loads the snapshot and trailing events for an
// aggregate
func (p *Persistence) LoadSnapshot(
	id timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	ctx := context.Background()
	key, _, err := aggregateKey(id)
	if err != nil {
		return nil, err
	}

	var snapData string
	var snapSeq int64
	err = p.pool.QueryRow(ctx, `
		SELECT snapshot_data, snapshot_seq
		FROM timebox_snapshot
		WHERE store = $1 AND aggregate_key = $2
	`, p.Prefix, key).Scan(&snapData, &snapSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		snapData = ""
		snapSeq = 0
	} else if err != nil {
		return nil, err
	}

	evs, err := p.loadEvents(ctx, key, snapSeq)
	if err != nil {
		return nil, err
	}
	return &timebox.SnapshotRecord{
		Data:     json.RawMessage(snapData),
		Sequence: snapSeq,
		Events:   evs,
	}, nil
}

// SaveSnapshot saves a snapshot if the provided sequence is not
// older
func (p *Persistence) SaveSnapshot(
	id timebox.AggregateID, data []byte, sequence int64,
) error {
	ctx := context.Background()
	key, parts, err := aggregateKey(id)
	if err != nil {
		return err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var baseSeq, snapSeq, nextSeq int64
	found, err := p.loadSnapshotState(
		ctx, tx, key, &baseSeq, &snapSeq, &nextSeq,
	)
	if err != nil {
		return err
	}
	if !found {
		if err := p.insertAggregate(ctx, tx, key, parts); err != nil {
			return err
		}
		found, err = p.loadSnapshotState(
			ctx, tx, key, &baseSeq, &snapSeq, &nextSeq,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: missing aggregate after insert",
				timebox.ErrUnexpectedResult,
			)
		}
	}
	if sequence < snapSeq {
		return nil
	}

	newBase := baseSeq
	if p.Timebox.Snapshot.TrimEvents && sequence > baseSeq {
		newBase = min(sequence, nextSeq)
		if newBase > baseSeq {
			if _, err := tx.Exec(ctx, `
				DELETE FROM timebox_events
				WHERE store = $1
				  AND aggregate_key = $2
				  AND sequence < $3
			`, p.Prefix, key, newBase); err != nil {
				return err
			}
		}
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO timebox_snapshot (
			store, aggregate_key, base_seq,
			snapshot_seq, snapshot_data
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (store, aggregate_key) DO UPDATE
		SET base_seq = EXCLUDED.base_seq,
		    snapshot_seq = EXCLUDED.snapshot_seq,
		    snapshot_data = EXCLUDED.snapshot_data
	`, p.Prefix, key, newBase, sequence, string(data)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListAggregates lists aggregate IDs matching the given prefix
func (p *Persistence) ListAggregates(
	id timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	ctx := context.Background()

	var rows pgx.Rows
	var err error
	if len(id) == 0 {
		rows, err = p.pool.Query(ctx, `
			SELECT aggregate_parts
			FROM timebox_index
			WHERE store = $1
		`, p.Prefix)
	} else {
		rows, err = p.pool.Query(ctx, `
			SELECT aggregate_parts
			FROM timebox_index
			WHERE store = $1
			  AND array_length(aggregate_parts, 1) >= $2
			  AND aggregate_parts[1:$2] = $3::text[]
		`, p.Prefix, len(id), stringParts(id))
	}
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

func (p *Persistence) loadSnapshotState(
	ctx context.Context, tx pgx.Tx, key string,
	baseSeq, snapSeq, nextSeq *int64,
) (bool, error) {
	err := tx.QueryRow(ctx, `
		SELECT COALESCE(s.base_seq, 0),
		       COALESCE(s.snapshot_seq, 0),
		       COALESCE((
		           SELECT e.sequence + 1
		           FROM timebox_events e
		           WHERE e.store = $1
		             AND e.aggregate_key = $2
		           ORDER BY e.sequence DESC
		           LIMIT 1
		       ), COALESCE(s.base_seq, 0))
		FROM timebox_index i
		LEFT JOIN timebox_snapshot s
		  ON s.store = i.store
		  AND s.aggregate_key = i.aggregate_key
		WHERE i.store = $1 AND i.aggregate_key = $2
		FOR UPDATE OF i
	`, p.Prefix, key).Scan(baseSeq, snapSeq, nextSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (p *Persistence) insertAggregate(
	ctx context.Context, tx pgx.Tx, key string, parts []string,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO timebox_index (
			store, aggregate_key, aggregate_parts, labels
		) VALUES ($1, $2, $3, '{}'::jsonb)
		ON CONFLICT (store, aggregate_key) DO NOTHING
	`, p.Prefix, key, parts)
	return err
}

func (p *Persistence) loadEvents(
	ctx context.Context, key string, fromSeq int64,
) ([]*timebox.Event, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT data
		FROM timebox_events
		WHERE store = $1
		  AND aggregate_key = $2
		  AND sequence >= $3
		ORDER BY sequence
	`, p.Prefix, key, fromSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []*timebox.Event
	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err != nil {
			return nil, err
		}
		ev, err := timebox.JSONEvent.Decode([]byte(msg))
		if err != nil {
			return nil, err
		}
		res = append(res, ev)
	}
	return res, rows.Err()
}

func aggregateKey(id timebox.AggregateID) (string, []string, error) {
	parts := stringParts(id)
	data, err := json.Marshal(parts)
	if err != nil {
		return "", nil, err
	}
	return string(data), parts, nil
}

func stringParts(id timebox.AggregateID) []string {
	res := make([]string, 0, len(id))
	for _, part := range id {
		res = append(res, string(part))
	}
	return res
}

func aggregateID(parts []string) timebox.AggregateID {
	res := make(timebox.AggregateID, 0, len(parts))
	for _, part := range parts {
		res = append(res, timebox.ID(part))
	}
	return res
}

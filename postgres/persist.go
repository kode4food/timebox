package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/id"
)

type (
	// Persistence implements timebox.Persistence using Postgres
	Persistence struct {
		timebox.AlwaysReady
		Config
		pool *pgxpool.Pool
	}

	// querier is the query surface shared by pgxpool.Pool and pgx.Tx, so one
	// append path serves a pooled call and a transaction alike
	querier interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
		QueryRow(context.Context, string, ...any) pgx.Row
	}
)

const defaultConnectTimeout = 5 * time.Second

var _ timebox.Backend = (*Persistence)(nil)

// NewPersistence creates Postgres-backed Persistence
func NewPersistence(cfgs ...Config) (*Persistence, error) {
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newPersistence(cfg)
}

// NewStore creates a Store using the current Postgres Persistence
func (p *Persistence) NewStore(cfg timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(p, cfg)
}

func newPersistence(cfg Config) (*Persistence, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(), defaultConnectTimeout,
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
		context.Background(), defaultSchemaTimeout,
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
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	ctx := context.Background()
	key, _ := aggregateKey(req.ID)

	var baseSeq int64
	var err error
	err = p.pool.QueryRow(ctx, `
		SELECT base_seq
		FROM timebox_snapshots
		WHERE store = $1 AND aggregate_key = $2
	`, p.Prefix, key).Scan(&baseSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		baseSeq = 0
	} else if err != nil {
		return nil, err
	}

	start := max(req.FromSeq, baseSeq)
	evs, err := p.loadEvents(ctx, p.pool, req.ID, key, start)
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
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	ctx := context.Background()
	key, _ := aggregateKey(req.ID)

	var snapData string
	var snapSeq int64
	var err error
	err = p.pool.QueryRow(ctx, `
		SELECT snapshot_data, snapshot_seq
		FROM timebox_snapshots
		WHERE store = $1 AND aggregate_key = $2
	`, p.Prefix, key).Scan(&snapData, &snapSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		snapData = ""
		snapSeq = 0
	} else if err != nil {
		return nil, err
	}

	evs, err := p.loadEvents(ctx, p.pool, req.ID, key, snapSeq)
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
	req timebox.SnapshotRequest,
) error {
	ctx := context.Background()
	key, parts := aggregateKey(req.ID)
	var err error
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
	if req.Sequence < snapSeq {
		return nil
	}

	newBase := baseSeq
	if req.Config().TrimEvents && req.Sequence > baseSeq {
		newBase = min(req.Sequence, nextSeq)
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
		INSERT INTO timebox_snapshots (
			store, aggregate_key, base_seq,
			snapshot_seq, snapshot_data
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (store, aggregate_key) DO UPDATE
		SET base_seq = EXCLUDED.base_seq,
		    snapshot_seq = EXCLUDED.snapshot_seq,
		    snapshot_data = EXCLUDED.snapshot_data
	`, p.Prefix, key, newBase, req.Sequence, req.Data); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListAggregates lists aggregate IDs matching the given prefix
func (p *Persistence) ListAggregates(
	prefix timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	ctx := context.Background()

	var rows pgx.Rows
	var err error
	parts := id.Parts[string](prefix)
	if len(parts) == 0 {
		rows, err = p.pool.Query(ctx, `
			SELECT aggregate_parts
			FROM timebox_statuses
			WHERE store = $1
		`, p.Prefix)
	} else {
		rows, err = p.pool.Query(ctx, `
			SELECT aggregate_parts
			FROM timebox_statuses
			WHERE store = $1
			  AND array_length(aggregate_parts, 1) >= $2
			  AND aggregate_parts[1:$2] = $3::text[]
		`, p.Prefix, len(parts), parts)
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
		aggID, err := timebox.AggregateIDFromParts(parts)
		if err != nil {
			return nil, err
		}
		res = append(res, aggID)
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
		FROM timebox_statuses i
		LEFT JOIN timebox_snapshots s
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
		INSERT INTO timebox_statuses (
			store, aggregate_key, aggregate_parts
		) VALUES ($1, $2, $3)
		ON CONFLICT (store, aggregate_key) DO NOTHING
	`, p.Prefix, key, parts)
	return err
}

func (p *Persistence) loadEvents(
	ctx context.Context, q querier, id timebox.AggregateID, key string,
	fromSeq int64,
) ([]*timebox.Event, error) {
	rows, err := q.Query(ctx, `
		SELECT sequence, event_at, event_type, data
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
		var seq int64
		var at int64
		var typ string
		var data string
		if err := rows.Scan(&seq, &at, &typ, &data); err != nil {
			return nil, err
		}
		res = append(res, &timebox.Event{
			Timestamp:   time.Unix(0, at).UTC(),
			Sequence:    seq,
			Type:        timebox.EventType(typ),
			AggregateID: id,
			Data:        json.RawMessage(data),
		})
	}
	return res, rows.Err()
}

func aggregateKey(aggID timebox.AggregateID) (string, []string) {
	parts := id.Parts[string](aggID)
	if len(parts) == 0 {
		return "", nil
	}
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
		b.WriteByte(';')
	}
	return b.String(), parts
}

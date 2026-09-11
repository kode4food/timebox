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
)

type (
	// Backend implements timebox.Backend using Postgres
	Backend struct {
		timebox.AlwaysReady
		pool *pgxpool.Pool
	}

	// querier is the query surface shared by pgxpool.Pool and pgx.Tx, so one
	// append path serves a pooled call and a transaction alike
	querier interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
		QueryRow(context.Context, string, ...any) pgx.Row
	}

	snapshotState struct {
		baseSeq int64
		snapSeq int64
		nextSeq int64
	}

	eventRange struct {
		id      timebox.AggregateID
		key     string
		fromSeq int64
	}
)

const defaultConnectTimeout = 5 * time.Second

var _ timebox.Backend = (*Backend)(nil)

// Open opens Postgres-backed Backend
func Open(cfgs ...Config) (*Backend, error) {
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newBackend(cfg)
}

// NewStore creates a Store using the current Postgres Backend
func (b *Backend) NewStore(cfgs ...timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(b, cfgs...)
}

func newBackend(cfg Config) (*Backend, error) {
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

	poolCfg.ConnConfig.RuntimeParams["search_path"] =
		pgx.Identifier{cfg.Prefix}.Sanitize()

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

	if err := initSchema(schemaCtx, pool, cfg.Prefix); err != nil {
		pool.Close()
		return nil, err
	}

	return &Backend{pool: pool}, nil
}

// Close closes the Postgres connection pool
func (b *Backend) Close() error {
	b.pool.Close()
	return nil
}

// LoadEvents loads events starting at fromSeq
func (b *Backend) LoadEvents(
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	ctx := context.Background()
	key, _ := aggregateKey(req.ID)

	var baseSeq int64
	err := b.pool.QueryRow(ctx, sqlSnapshotBaseSeq, key).Scan(&baseSeq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	start := max(req.FromSeq, baseSeq)
	evs, err := b.loadEvents(ctx, b.pool, eventRange{
		id:      req.ID,
		key:     key,
		fromSeq: start,
	})
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
func (b *Backend) LoadSnapshot(
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	ctx := context.Background()
	key, _ := aggregateKey(req.ID)

	var snapData []byte
	var snapSeq int64
	err := b.pool.QueryRow(ctx, sqlGetSnapshot, key).
		Scan(&snapData, &snapSeq)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	evs, err := b.loadEvents(ctx, b.pool, eventRange{
		id:      req.ID,
		key:     key,
		fromSeq: snapSeq,
	})
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
func (b *Backend) SaveSnapshot(
	req timebox.SnapshotRequest,
) error {
	ctx := context.Background()
	key, parts := aggregateKey(req.ID)
	var err error
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	state, found, err := b.loadSnapshotState(ctx, tx, key)
	if err != nil {
		return err
	}
	if !found {
		if err := b.insertAggregate(ctx, tx, key, parts); err != nil {
			return err
		}
		state, found, err = b.loadSnapshotState(ctx, tx, key)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: missing aggregate after insert",
				timebox.ErrUnexpectedResult,
			)
		}
	}
	if req.Sequence < state.snapSeq {
		return nil
	}

	newBase := state.baseSeq
	if req.TrimEvents && req.Sequence > state.baseSeq {
		newBase = min(req.Sequence, state.nextSeq)
		if newBase > state.baseSeq {
			_, err := tx.Exec(ctx, sqlTrimEvents, key, newBase)
			if err != nil {
				return err
			}
		}
	}

	if _, err = tx.Exec(ctx, sqlPutSnapshot,
		key, newBase, req.Sequence, req.Data,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListAggregates lists aggregate IDs of the given type, or of every type when
// it is empty
func (b *Backend) ListAggregates(
	typ timebox.ID,
) ([]timebox.AggregateID, error) {
	ctx := context.Background()

	var rows pgx.Rows
	var err error
	if typ == "" {
		rows, err = b.pool.Query(ctx, sqlListAggregates)
	} else {
		rows, err = b.pool.Query(ctx, sqlListAggregatesByType, string(typ))
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
		aggID, err := aggregateID(parts)
		if err != nil {
			return nil, err
		}
		res = append(res, aggID)
	}
	return res, rows.Err()
}

func (b *Backend) loadSnapshotState(
	ctx context.Context, tx pgx.Tx, key string,
) (snapshotState, bool, error) {
	var res snapshotState
	err := tx.QueryRow(ctx, sqlSnapshotState, key).
		Scan(&res.baseSeq, &res.snapSeq, &res.nextSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return snapshotState{}, false, nil
	}
	if err != nil {
		return snapshotState{}, false, err
	}
	return res, true, nil
}

func (b *Backend) insertAggregate(
	ctx context.Context, tx pgx.Tx, key string, parts []string,
) error {
	_, err := tx.Exec(ctx, sqlInsertAggregate, key, parts)
	return err
}

func (b *Backend) loadEvents(
	ctx context.Context, q querier, r eventRange,
) ([]*timebox.Event, error) {
	rows, err := q.Query(ctx, sqlGetEvents, r.key, r.fromSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []*timebox.Event
	for rows.Next() {
		var seq int64
		var at int64
		var typ string
		var data []byte
		if err := rows.Scan(&seq, &at, &typ, &data); err != nil {
			return nil, err
		}
		res = append(res, &timebox.Event{
			Timestamp:   time.Unix(0, at).UTC(),
			Sequence:    seq,
			Type:        timebox.EventType(typ),
			AggregateID: r.id,
			Data:        json.RawMessage(data),
		})
	}
	return res, rows.Err()
}

func aggregateID(parts []string) (timebox.AggregateID, error) {
	if len(parts) != 2 {
		return timebox.AggregateID{}, timebox.ErrInvalidAggregateID
	}
	return timebox.NewAggregateID(
		timebox.ID(parts[0]), timebox.ID(parts[1]),
	), nil
}

func aggregateKey(id timebox.AggregateID) (string, []string) {
	parts := []string{string(id.Type), string(id.Key)}
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
		b.WriteByte(';')
	}
	return b.String(), parts
}

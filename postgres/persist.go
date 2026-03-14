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
	// Persistence implements timebox.Persistence using Postgres
	Persistence struct {
		pool  *pgxpool.Pool
		store string
		cfg   Config
	}

	aggregateRef struct {
		key   string
		parts []string
	}

	aggregateRow struct {
		baseSeq     int64
		snapshotSeq int64
		snapshot    string
		status      string
		statusAt    int64
		labels      map[string]string
		nextSeq     int64
	}

	rowQuery interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}

	queryRunner interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}

	appendFunctionSpec struct {
		name   string
		status bool
		labels bool
	}
)

const (
	connectTimeout = 5 * time.Second
	schemaTimeout  = 60 * time.Second
	pollInterval   = 50 * time.Millisecond
	archiveLease   = 30 * time.Second
)

var schemaStatements = func() []string {
	stmts := []string{
		`
CREATE TABLE IF NOT EXISTS timebox_index (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	aggregate_parts TEXT[] NOT NULL,
	status TEXT NOT NULL DEFAULT '',
	status_at BIGINT NOT NULL DEFAULT 0,
	labels JSONB NOT NULL DEFAULT '{}'::jsonb,
	PRIMARY KEY (store, aggregate_key)
)`,
		`
CREATE INDEX IF NOT EXISTS timebox_index_status_idx
	ON timebox_index (store, status, status_at, aggregate_key)
`,
		`
CREATE TABLE IF NOT EXISTS timebox_events (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	sequence BIGINT NOT NULL,
	data TEXT NOT NULL,
	PRIMARY KEY (store, aggregate_key, sequence)
)`,
		`
CREATE TABLE IF NOT EXISTS timebox_snapshot (
	store TEXT NOT NULL,
	aggregate_key TEXT NOT NULL,
	base_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_seq BIGINT NOT NULL DEFAULT 0,
	snapshot_data TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (store, aggregate_key)
)`,
		`
CREATE TABLE IF NOT EXISTS timebox_archive (
	store TEXT NOT NULL,
	stream_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	aggregate_parts TEXT[] NOT NULL,
	snapshot_data TEXT NOT NULL DEFAULT '',
	snapshot_sequence BIGINT NOT NULL DEFAULT 0,
	events TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
	lease_until TIMESTAMPTZ
)`,
		`
CREATE INDEX IF NOT EXISTS timebox_archive_claim_idx
	ON timebox_archive (store, lease_until, stream_id)
`,
		`DROP FUNCTION IF EXISTS timebox_append(TEXT, TEXT, TEXT[], BIGINT, TEXT, BIGINT, JSONB, TEXT[])`,
	}
	for _, spec := range []appendFunctionSpec{
		{name: "timebox_append_plain"},
		{name: "timebox_append_status", status: true},
		{name: "timebox_append_labels", labels: true},
		{name: "timebox_append_status_labels", status: true, labels: true},
	} {
		stmts = append(stmts, buildAppendFunctionSQL(spec))
	}
	return stmts
}()

const (
	appendPlainQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_plain($1, $2, $3, $4, $5::text[])
	`

	appendStatusQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_status($1, $2, $3, $4, $5, $6, $7::text[])
	`

	appendLabelsQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_labels($1, $2, $3, $4, $5::jsonb, $6::text[])
	`

	appendStatusLabelsQuery = `
		SELECT success, actual_sequence, new_events
		FROM timebox_append_status_labels(
			$1, $2, $3, $4, $5, $6, $7::jsonb, $8::text[]
		)
	`
)

var _ timebox.Persistence = (*Persistence)(nil)

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
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	schemaCtx, cancel := context.WithTimeout(context.Background(), schemaTimeout)
	defer cancel()

	if err := initSchema(schemaCtx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return &Persistence{
		pool:  pool,
		store: cfg.Prefix,
		cfg:   cfg,
	}, nil
}

// Close closes the Postgres connection pool
func (p *Persistence) Close() error {
	p.pool.Close()
	return nil
}

// Append appends events if the expected sequence matches
func (p *Persistence) Append(
	req timebox.AppendRequest,
) (_ *timebox.AppendResult, err error) {
	ctx := context.Background()
	ref, err := makeAggregateRef(req.ID)
	if err != nil {
		return nil, err
	}

	var (
		status   any
		statusAt int64
	)
	if req.Status != nil {
		status = *req.Status
		if req.StatusAt != "" {
			if v, err := strconv.ParseInt(req.StatusAt, 10, 64); err == nil {
				statusAt = v
			}
		}
	}

	var (
		success   bool
		actualSeq int64
		newEvents []string
	)
	switch {
	case req.Status != nil && len(req.Labels) > 0:
		lblData, err := json.Marshal(req.Labels)
		if err != nil {
			return nil, err
		}
		err = p.pool.QueryRow(ctx, appendStatusLabelsQuery,
			p.store, ref.key, ref.parts, req.ExpectedSequence, status,
			statusAt, string(lblData), req.Events,
		).Scan(&success, &actualSeq, &newEvents)
	case req.Status != nil:
		err = p.pool.QueryRow(ctx, appendStatusQuery,
			p.store, ref.key, ref.parts, req.ExpectedSequence, status,
			statusAt, req.Events,
		).Scan(&success, &actualSeq, &newEvents)
	case len(req.Labels) > 0:
		lblData, err := json.Marshal(req.Labels)
		if err != nil {
			return nil, err
		}
		err = p.pool.QueryRow(ctx, appendLabelsQuery,
			p.store, ref.key, ref.parts, req.ExpectedSequence,
			string(lblData), req.Events,
		).Scan(&success, &actualSeq, &newEvents)
	default:
		err = p.pool.QueryRow(ctx, appendPlainQuery,
			p.store, ref.key, ref.parts, req.ExpectedSequence, req.Events,
		).Scan(&success, &actualSeq, &newEvents)
	}
	if err != nil {
		return nil, err
	}
	if success {
		return nil, nil
	}
	return &timebox.AppendResult{
		ActualSequence: actualSeq,
		NewEvents:      makeRawMessages(newEvents),
	}, nil
}

// LoadEvents loads events starting at fromSeq
func (p *Persistence) LoadEvents(
	id timebox.AggregateID, fromSeq int64,
) (*timebox.EventsResult, error) {
	ctx := context.Background()
	ref, err := makeAggregateRef(id)
	if err != nil {
		return nil, err
	}

	var baseSeq int64
	err = p.pool.QueryRow(ctx, `
		SELECT base_seq
		FROM timebox_snapshot
		WHERE store = $1 AND aggregate_key = $2
	`, p.store, ref.key).Scan(&baseSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		baseSeq = 0
	} else if err != nil {
		return nil, err
	}

	start := max(fromSeq, baseSeq)
	evs, err := p.loadEvents(ctx, p.pool, ref.key, start)
	if err != nil {
		return nil, err
	}
	return &timebox.EventsResult{
		StartSequence: start,
		Events:        evs,
	}, nil
}

// LoadSnapshot loads the snapshot and trailing events for an aggregate
func (p *Persistence) LoadSnapshot(
	id timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	ctx := context.Background()
	ref, err := makeAggregateRef(id)
	if err != nil {
		return nil, err
	}

	var snapData string
	var snapSeq int64
	err = p.pool.QueryRow(ctx, `
		SELECT snapshot_data, snapshot_seq
		FROM timebox_snapshot
		WHERE store = $1 AND aggregate_key = $2
	`, p.store, ref.key).Scan(&snapData, &snapSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		snapData = ""
		snapSeq = 0
	} else if err != nil {
		return nil, err
	}

	evs, err := p.loadEvents(ctx, p.pool, ref.key, snapSeq)
	if err != nil {
		return nil, err
	}
	return &timebox.SnapshotRecord{
		Data:     json.RawMessage(snapData),
		Sequence: snapSeq,
		Events:   evs,
	}, nil
}

// SaveSnapshot saves a snapshot if the provided sequence is not older
func (p *Persistence) SaveSnapshot(
	ctx context.Context, id timebox.AggregateID, data []byte, sequence int64,
) (_ error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	ref, err := makeAggregateRef(id)
	if err != nil {
		return err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	row, found, err := p.loadAggregate(ctx, tx, ref.key)
	if err != nil {
		return err
	}
	if !found {
		if err := p.insertAggregate(ctx, tx, ref); err != nil {
			return err
		}
		row, found, err = p.loadAggregate(ctx, tx, ref.key)
		if err != nil {
			return err
		}
		if !found {
			return errors.Join(
				timebox.ErrUnexpectedResult,
				errors.New("missing aggregate after insert"),
			)
		}
	}
	if sequence < row.snapshotSeq {
		return tx.Rollback(ctx)
	}

	newBase := row.baseSeq
	if p.cfg.Timebox.Snapshot.TrimEvents && sequence > row.baseSeq {
		newBase = min(sequence, row.nextSeq)
		if newBase > row.baseSeq {
			if _, err := tx.Exec(ctx, `
				DELETE FROM timebox_events
				WHERE store = $1 AND aggregate_key = $2 AND sequence < $3
			`, p.store, ref.key, newBase); err != nil {
				return err
			}
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO timebox_snapshot (
			store, aggregate_key, base_seq, snapshot_seq, snapshot_data
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (store, aggregate_key) DO UPDATE
		SET base_seq = EXCLUDED.base_seq,
		    snapshot_seq = EXCLUDED.snapshot_seq,
		    snapshot_data = EXCLUDED.snapshot_data
	`, p.store, ref.key, newBase, sequence, string(data)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ListAggregates lists aggregate IDs matching the given prefix
func (p *Persistence) ListAggregates(
	id timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	ctx := context.Background()

	var (
		rows pgx.Rows
		err  error
	)
	if len(id) == 0 {
		rows, err = p.pool.Query(ctx, `
			SELECT aggregate_parts
			FROM timebox_index
			WHERE store = $1
			ORDER BY aggregate_key
		`, p.store)
	} else {
		rows, err = p.pool.Query(ctx, `
			SELECT aggregate_parts
			FROM timebox_index
			WHERE store = $1
			  AND array_length(aggregate_parts, 1) >= $2
			  AND aggregate_parts[1:$2] = $3::text[]
			ORDER BY aggregate_key
		`, p.store, len(id), aggregateParts(id))
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
		res = append(res, makeAggregateID(parts))
	}
	return res, rows.Err()
}

// GetAggregateStatus gets the current status for an aggregate
func (p *Persistence) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	ctx := context.Background()
	ref, err := makeAggregateRef(id)
	if err != nil {
		return "", err
	}

	var status string
	err = p.pool.QueryRow(ctx, `
		SELECT status
		FROM timebox_index
		WHERE store = $1 AND aggregate_key = $2
	`, p.store, ref.key).Scan(&status)
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
		FROM timebox_index
		WHERE store = $1 AND status = $2
		ORDER BY aggregate_key
	`, p.store, status)
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
			ID:        makeAggregateID(parts),
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
		SELECT aggregate_parts
		FROM timebox_index
		WHERE store = $1 AND labels ->> $2 = $3
		ORDER BY aggregate_key
	`, p.store, label, value)
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
		res = append(res, makeAggregateID(parts))
	}
	return res, rows.Err()
}

// ListLabelValues lists values currently used for a label
func (p *Persistence) ListLabelValues(label string) ([]string, error) {
	rows, err := p.pool.Query(context.Background(), `
		SELECT DISTINCT lbl.value
		FROM timebox_index i
		CROSS JOIN LATERAL jsonb_each_text(i.labels) AS lbl(key, value)
		WHERE i.store = $1 AND lbl.key = $2
		ORDER BY lbl.value
	`, p.store, label)
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

// Archive archives an aggregate and removes it from active storage
func (p *Persistence) Archive(id timebox.AggregateID) (_ error) {
	if !p.cfg.Timebox.Archiving {
		return timebox.ErrArchivingDisabled
	}

	ctx := context.Background()
	ref, err := makeAggregateRef(id)
	if err != nil {
		return err
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	var parts []string
	err = tx.QueryRow(ctx, `
		SELECT aggregate_parts
		FROM timebox_index
		WHERE store = $1 AND aggregate_key = $2
		FOR UPDATE
	`, p.store, ref.key).Scan(&parts)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Rollback(ctx)
	}
	if err != nil {
		return err
	}

	var snapData string
	var snapSeq int64
	err = tx.QueryRow(ctx, `
		SELECT snapshot_data, snapshot_seq
		FROM timebox_snapshot
		WHERE store = $1 AND aggregate_key = $2
	`, p.store, ref.key).Scan(&snapData, &snapSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		snapData = ""
		snapSeq = 0
	} else if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, `
		SELECT data
		FROM timebox_events
		WHERE store = $1 AND aggregate_key = $2
		ORDER BY sequence
	`, p.store, ref.key)
	if err != nil {
		return err
	}
	evs, err := scanMessageStrings(rows)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO timebox_archive (
			store, aggregate_parts, snapshot_data, snapshot_sequence, events
		) VALUES ($1, $2, $3, $4, $5)
	`, p.store, parts, snapData, snapSeq, evs); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM timebox_events
		WHERE store = $1 AND aggregate_key = $2
	`, p.store, ref.key); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM timebox_snapshot
		WHERE store = $1 AND aggregate_key = $2
	`, p.store, ref.key); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM timebox_index
		WHERE store = $1 AND aggregate_key = $2
	`, p.store, ref.key); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// ConsumeArchive blocks until an archive record is available
func (p *Persistence) ConsumeArchive(
	ctx context.Context, h timebox.ArchiveHandler,
) error {
	return p.PollArchive(ctx, 0, h)
}

// PollArchive waits up to timeout for one archive record
func (p *Persistence) PollArchive(
	ctx context.Context, timeout time.Duration, h timebox.ArchiveHandler,
) error {
	if !p.cfg.Timebox.Archiving {
		return timebox.ErrArchivingDisabled
	}
	if h == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		rec, err := p.claimArchive(ctx)
		if err != nil {
			return err
		}
		if rec != nil {
			if err := h(ctx, rec); err != nil {
				return err
			}
			return p.consumeArchive(ctx, rec.StreamID)
		}

		if timeout > 0 && !time.Now().Before(deadline) {
			return nil
		}

		waitFor := pollInterval
		if timeout > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil
			}
			waitFor = minDuration(waitFor, remaining)
		}

		timer := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *Persistence) claimArchive(
	ctx context.Context,
) (*timebox.ArchiveRecord, error) {
	row := p.pool.QueryRow(ctx, `
		WITH next AS (
			SELECT stream_id
			FROM timebox_archive
			WHERE store = $1
			  AND (lease_until IS NULL OR lease_until < NOW())
			ORDER BY stream_id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE timebox_archive a
		SET lease_until = NOW() + ($2 * INTERVAL '1 millisecond')
		FROM next
		WHERE a.stream_id = next.stream_id
		RETURNING a.stream_id, a.aggregate_parts, a.snapshot_data,
		          a.snapshot_sequence, a.events
	`, p.store, archiveLease.Milliseconds())

	var streamID int64
	var parts []string
	var snapData string
	var snapSeq int64
	var evs []string
	err := row.Scan(&streamID, &parts, &snapData, &snapSeq, &evs)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &timebox.ArchiveRecord{
		StreamID:         strconv.FormatInt(streamID, 10),
		AggregateID:      makeAggregateID(parts),
		SnapshotData:     json.RawMessage(snapData),
		SnapshotSequence: snapSeq,
		Events:           makeRawMessages(evs),
	}, nil
}

func (p *Persistence) consumeArchive(
	ctx context.Context, streamID string,
) error {
	id, err := strconv.ParseInt(streamID, 10, 64)
	if err != nil {
		return err
	}
	_, err = p.pool.Exec(ctx, `
		DELETE FROM timebox_archive
		WHERE store = $1 AND stream_id = $2
	`, p.store, id)
	return err
}

func (p *Persistence) insertAggregate(
	ctx context.Context, tx pgx.Tx, ref *aggregateRef,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO timebox_index (
			store, aggregate_key, aggregate_parts, labels
		) VALUES ($1, $2, $3, '{}'::jsonb)
		ON CONFLICT (store, aggregate_key) DO NOTHING
	`, p.store, ref.key, ref.parts)
	return err
}

func (p *Persistence) loadAggregate(
	ctx context.Context, q rowQuery, key string,
) (*aggregateRow, bool, error) {
	var (
		lbls []byte
		row  aggregateRow
	)

	err := q.QueryRow(ctx, `
		SELECT COALESCE(s.base_seq, 0),
		       COALESCE(s.snapshot_seq, 0),
		       COALESCE(s.snapshot_data, ''),
		       i.status,
		       i.status_at,
		       i.labels,
		       COALESCE((
		           SELECT e.sequence + 1
		           FROM timebox_events e
		           WHERE e.store = $1 AND e.aggregate_key = $2
		           ORDER BY e.sequence DESC
		           LIMIT 1
		       ), COALESCE(s.base_seq, 0))
		FROM timebox_index i
		LEFT JOIN timebox_snapshot s
		  ON s.store = i.store AND s.aggregate_key = i.aggregate_key
		WHERE i.store = $1 AND i.aggregate_key = $2
		FOR UPDATE OF i
	`, p.store, key).Scan(
		&row.baseSeq,
		&row.snapshotSeq,
		&row.snapshot,
		&row.status,
		&row.statusAt,
		&lbls,
		&row.nextSeq,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	row.labels = map[string]string{}
	if len(lbls) != 0 {
		if err := json.Unmarshal(lbls, &row.labels); err != nil {
			return nil, false, err
		}
	}
	return &row, true, nil
}

func (p *Persistence) loadTrailingEvents(
	ctx context.Context, q queryRunner, key string, fromSeq int64,
) ([]json.RawMessage, error) {
	return p.loadEvents(ctx, q, key, fromSeq)
}

func (p *Persistence) loadEvents(
	ctx context.Context, q queryRunner, key string, fromSeq int64,
) ([]json.RawMessage, error) {
	rows, err := q.Query(ctx, `
		SELECT data
		FROM timebox_events
		WHERE store = $1 AND aggregate_key = $2 AND sequence >= $3
		ORDER BY sequence
	`, p.store, key, fromSeq)
	if err != nil {
		return nil, err
	}
	msgs, err := scanMessageStrings(rows)
	if err != nil {
		return nil, err
	}
	return makeRawMessages(msgs), nil
}

func scanMessageStrings(rows pgx.Rows) ([]string, error) {
	defer rows.Close()

	var res []string
	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err != nil {
			return nil, err
		}
		res = append(res, msg)
	}
	return res, rows.Err()
}

func makeRawMessages(in []string) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(in))
	for _, item := range in {
		out = append(out, json.RawMessage(item))
	}
	return out
}

func aggregateKey(id timebox.AggregateID) (string, []string, error) {
	parts := aggregateParts(id)
	data, err := json.Marshal(parts)
	if err != nil {
		return "", nil, err
	}
	return string(data), parts, nil
}

func makeAggregateRef(id timebox.AggregateID) (*aggregateRef, error) {
	key, parts, err := aggregateKey(id)
	if err != nil {
		return nil, err
	}
	return &aggregateRef{
		key:   key,
		parts: parts,
	}, nil
}

func aggregateParts(id timebox.AggregateID) []string {
	res := make([]string, 0, len(id))
	for _, part := range id {
		res = append(res, string(part))
	}
	return res
}

func makeAggregateID(parts []string) timebox.AggregateID {
	res := make(timebox.AggregateID, 0, len(parts))
	for _, part := range parts {
		res = append(res, timebox.ID(part))
	}
	return res
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func buildAppendFunctionSQL(spec appendFunctionSpec) string {
	args := []string{
		"p_store TEXT",
		"p_aggregate_key TEXT",
		"p_aggregate_parts TEXT[]",
		"p_expected_sequence BIGINT",
	}
	if spec.status {
		args = append(args, "p_status TEXT", "p_status_at BIGINT")
	}
	if spec.labels {
		args = append(args, "p_labels JSONB")
	}
	args = append(args, "p_events TEXT[]")

	decls := []string{
		"v_base_seq BIGINT := 0;",
		"v_snapshot_seq BIGINT := 0;",
		"v_next_seq BIGINT := 0;",
		"v_current_seq BIGINT := 0;",
		"v_event_count BIGINT := COALESCE(array_length(p_events, 1), 0);",
	}
	if spec.labels {
		decls = append(decls,
			"v_labels JSONB := '{}'::jsonb;",
			"v_removed_keys TEXT[] := ARRAY[]::TEXT[];",
			"v_set_labels JSONB := '{}'::jsonb;",
		)
	}

	selectExprs := []string{
		"COALESCE(s.base_seq, 0)",
		"COALESCE(s.snapshot_seq, 0)",
	}
	intoVars := []string{"v_base_seq", "v_snapshot_seq"}
	if spec.labels {
		selectExprs = append(selectExprs, "i.labels")
		intoVars = append(intoVars, "v_labels")
	}
	selectExprs = append(selectExprs, `
       COALESCE((
           SELECT e.sequence + 1
           FROM timebox_events e
           WHERE e.store = p_store AND e.aggregate_key = p_aggregate_key
           ORDER BY e.sequence DESC
           LIMIT 1
       ), COALESCE(s.base_seq, 0))`)
	intoVars = append(intoVars, "v_next_seq")

	var update strings.Builder
	if spec.labels {
		update.WriteString(`
	SELECT COALESCE(array_agg(kv.key), ARRAY[]::TEXT[])
	INTO v_removed_keys
	FROM jsonb_each_text(p_labels) AS kv
	WHERE kv.value = '';

	SELECT COALESCE(
		jsonb_object_agg(kv.key, to_jsonb(kv.value)),
		'{}'::jsonb
	)
	INTO v_set_labels
	FROM jsonb_each_text(p_labels) AS kv
	WHERE kv.value <> '';
`)
	}
	assignments := []string{}
	if spec.status {
		assignments = append(assignments,
			"status = p_status",
			"status_at = COALESCE(p_status_at, 0)",
		)
	}
	if spec.labels {
		assignments = append(assignments,
			"labels = (v_labels - v_removed_keys) || v_set_labels",
		)
	}
	if len(assignments) > 0 {
		update.WriteString("UPDATE timebox_index\nSET ")
		update.WriteString(strings.Join(assignments, ",\n    "))
		update.WriteString(`
WHERE store = p_store AND aggregate_key = p_aggregate_key;
`)
	}

	return fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s(
	%s
) RETURNS TABLE(
	success BOOLEAN,
	actual_sequence BIGINT,
	new_events TEXT[]
) AS $$
DECLARE
	%s
BEGIN
	IF p_expected_sequence = 0 THEN
		INSERT INTO timebox_index (
			store, aggregate_key, aggregate_parts, labels
		) VALUES (p_store, p_aggregate_key, p_aggregate_parts, '{}'::jsonb)
		ON CONFLICT (store, aggregate_key) DO NOTHING;
	END IF;

	SELECT %s
	INTO %s
	FROM timebox_index i
	LEFT JOIN timebox_snapshot s
	  ON s.store = i.store AND s.aggregate_key = i.aggregate_key
	WHERE i.store = p_store AND i.aggregate_key = p_aggregate_key
	FOR UPDATE OF i;

	IF NOT FOUND THEN
		success := FALSE;
		actual_sequence := 0;
		new_events := ARRAY[]::TEXT[];
		RETURN NEXT;
		RETURN;
	END IF;

	v_current_seq := GREATEST(v_next_seq, v_snapshot_seq);
	IF p_expected_sequence <> v_current_seq THEN
		success := FALSE;
		actual_sequence := v_current_seq;
		SELECT COALESCE(array_agg(e.data ORDER BY e.sequence), ARRAY[]::TEXT[])
		INTO new_events
		FROM timebox_events e
		WHERE e.store = p_store
		  AND e.aggregate_key = p_aggregate_key
		  AND e.sequence >= p_expected_sequence;
		RETURN NEXT;
		RETURN;
	END IF;

	INSERT INTO timebox_events (store, aggregate_key, sequence, data)
	SELECT p_store, p_aggregate_key, p_expected_sequence + ev.ord - 1, ev.data
	FROM unnest(COALESCE(p_events, ARRAY[]::TEXT[])) WITH ORDINALITY AS ev(data, ord);
%s
	success := TRUE;
	actual_sequence := v_current_seq + v_event_count;
	new_events := ARRAY[]::TEXT[];
	RETURN NEXT;
END;
$$ LANGUAGE plpgsql
`,
		spec.name,
		strings.Join(args, ",\n\t"),
		strings.Join(decls, "\n\t"),
		strings.Join(selectExprs, ",\n       "),
		strings.Join(intoVars, ", "),
		update.String(),
	)
}

func initSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for _, stmt := range schemaStatements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

func TestPersistenceSchema(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		require.NoError(t, err)
		defer func() { _ = p.Close() }()

		pool, err := pgxpool.New(ctx, cfg.URL)
		require.NoError(t, err)
		defer pool.Close()

		var colCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'timebox_events'
			  AND column_name IN ('aggregate_kind', 'aggregate_id')
		`).Scan(&colCount)
		require.NoError(t, err)
		assert.Zero(t, colCount)

		var fkCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			WHERE t.relname IN (
				'timebox_events',
				'timebox_snapshot',
				'timebox_archive'
			) AND c.contype = 'f'
		`).Scan(&fkCount)
		require.NoError(t, err)
		assert.Zero(t, fkCount)

		var idxCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND indexname = 'timebox_events_lookup_idx'
		`).Scan(&idxCount)
		require.NoError(t, err)
		assert.Zero(t, idxCount)
	})
}

func TestStoreLifecycle(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		cfg.Timebox.Archiving = true
		cfg.Timebox.Snapshot.TrimEvents = true
		cfg.Timebox.Indexer = func(evs []*timebox.Event) []*timebox.Index {
			res := make([]*timebox.Index, 0, len(evs))
			for _, ev := range evs {
				data := struct {
					Status string `json:"status"`
					Env    string `json:"env"`
				}{}
				err := json.Unmarshal(ev.Data, &data)
				require.NoError(t, err)
				res = append(res, &timebox.Index{
					Status: &data.Status,
					Labels: map[string]string{"env": data.Env},
				})
			}
			return res
		}

		store, err := postgres.NewStore(cfg)
		require.NoError(t, err)
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "1")
		first := eventAt(t,
			time.Unix(1_700_000_000, 0).UTC(), "active", "dev", 1,
		)
		second := eventAt(t,
			time.Unix(1_700_000_001, 0).UTC(), "paused", "prod", 2,
		)

		require.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{first}))

		status, err := store.GetAggregateStatus(id)
		require.NoError(t, err)
		assert.Equal(t, "active", status)

		ids, err := store.ListAggregatesByLabel("env", "dev")
		require.NoError(t, err)
		assert.Equal(t, []timebox.AggregateID{id}, ids)

		statuses, err := store.ListAggregatesByStatus("active")
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		assert.Equal(t, id, statuses[0].ID)
		assert.Equal(t, first.Timestamp, statuses[0].Timestamp)

		require.NoError(t,
			store.PutSnapshot(id, map[string]int{"value": 1}, 1),
		)

		require.NoError(t, store.AppendEvents(id, 1, []*timebox.Event{second}))

		var snap map[string]int
		res, err := store.GetSnapshot(id, &snap)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"value": 1}, snap)
		assert.Equal(t, int64(1), res.NextSequence)
		require.Len(t, res.AdditionalEvents, 1)
		assert.Equal(t, second.Type, res.AdditionalEvents[0].Type)

		require.NoError(t, store.Archive(id))

		var rec *timebox.ArchiveRecord
		err = store.PollArchive(ctx, 50*time.Millisecond, func(
			_ context.Context, r *timebox.ArchiveRecord,
		) error {
			rec = r
			return nil
		})
		require.NoError(t, err)
		require.NotNil(t, rec)
		assert.Equal(t, id, rec.AggregateID)
		assert.Equal(t, int64(1), rec.SnapshotSequence)
		require.Len(t, rec.Events, 1)
	})
}

func TestStoreAppendConflict(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		require.NoError(t, err)
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "conflict")
		first := eventAt(
			t, time.Unix(1_700_000_000, 0).UTC(), "active", "dev", 1,
		)
		second := eventAt(
			t, time.Unix(1_700_000_001, 0).UTC(), "paused", "prod", 2,
		)
		stale := eventAt(
			t, time.Unix(1_700_000_002, 0).UTC(), "stale", "dev", 3,
		)

		require.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{
			first,
			second,
		}))

		err = store.AppendEvents(id, 1, []*timebox.Event{stale})
		require.Error(t, err)

		var conflict *timebox.VersionConflictError
		require.ErrorAs(t, err, &conflict)
		assert.Equal(t, int64(1), conflict.ExpectedSequence)
		assert.Equal(t, int64(2), conflict.ActualSequence)
		require.Len(t, conflict.NewEvents, 1)
		assert.Equal(t, second.Type, conflict.NewEvents[0].Type)
		assert.Equal(t, int64(1), conflict.NewEvents[0].Sequence)
	})
}

func withTestDatabase(
	t *testing.T, fn func(context.Context, postgres.Config),
) {
	t.Helper()

	ctx := context.Background()
	adminCfg, err := pgxpool.ParseConfig(postgres.DefaultURL)
	require.NoError(t, err)

	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer admin.Close()

	if err := admin.Ping(ctx); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}

	dbName := fmt.Sprintf("timebox_test_%d", time.Now().UnixNano())
	_, err = admin.Exec(ctx,
		"CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize(),
	)
	require.NoError(t, err)

	defer func() {
		_, _ = admin.Exec(ctx, `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid <> pg_backend_pid()
		`, dbName)
		_, _ = admin.Exec(ctx,
			"DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize(),
		)
	}()

	testCfg := adminCfg.Copy()
	testCfg.ConnConfig.Database = dbName

	cfg := postgres.DefaultConfig()
	cfg.URL = databaseURL(t, adminCfg.ConnString(), dbName)
	cfg.Prefix = "timebox-test"

	fn(ctx, cfg)
}

func databaseURL(t *testing.T, rawURL, dbName string) string {
	t.Helper()

	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	u.Path = "/" + dbName
	return u.String()
}

func eventAt(
	t *testing.T, at time.Time, status, env string, value int,
) *timebox.Event {
	t.Helper()

	data, err := json.Marshal(map[string]any{
		"status": status,
		"env":    env,
		"value":  value,
	})
	require.NoError(t, err)

	return &timebox.Event{
		Timestamp: at,
		Type:      "event.test",
		Data:      data,
	}
}

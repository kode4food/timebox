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

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

func TestReady(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		select {
		case <-p.Ready():
		default:
			t.Fatal("Ready channel should be closed")
		}
	})
}

func TestPersistenceSchema(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		pool, err := pgxpool.New(ctx, cfg.URL)
		if !assert.NoError(t, err) {
			return
		}
		defer pool.Close()

		var colCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'timebox_events'
			  AND column_name IN (
			      'aggregate_kind', 'aggregate_id'
			  )
		`).Scan(&colCount)
		if !assert.NoError(t, err) {
			return
		}
		assert.Zero(t, colCount)

		var fkCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			WHERE t.relname IN (
				'timebox_events', 'timebox_snapshot'
			) AND c.contype = 'f'
		`).Scan(&fkCount)
		if !assert.NoError(t, err) {
			return
		}
		assert.Zero(t, fkCount)

		var idxCount int
		err = pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_indexes
			WHERE schemaname = current_schema()
			  AND indexname = 'timebox_events_lookup_idx'
		`).Scan(&idxCount)
		if !assert.NoError(t, err) {
			return
		}
		assert.Zero(t, idxCount)
	})
}

func TestStoreLifecycle(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		cfg.Timebox.Snapshot.TrimEvents = true
		cfg.Timebox.Indexer = statusEnvIndexer(t)

		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "1")
		first := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"active", "dev", 1,
		)
		second := testEvent(t,
			time.Unix(1_700_000_001, 0).UTC(),
			"paused", "prod", 2,
		)

		if !assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{first}),
		) {
			return
		}

		status, err := store.GetAggregateStatus(id)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, "active", status)

		ids, err := store.ListAggregatesByLabel("env", "dev")
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, []timebox.AggregateID{id}, ids)

		statuses, err := store.ListAggregatesByStatus("active")
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, statuses, 1) {
			return
		}
		assert.Equal(t, id, statuses[0].ID)
		assert.Equal(t, first.Timestamp, statuses[0].Timestamp)

		if !assert.NoError(t,
			store.PutSnapshot(
				id, map[string]int{"value": 1}, 1,
			),
		) {
			return
		}

		if !assert.NoError(t,
			store.AppendEvents(
				id, 1, []*timebox.Event{second},
			),
		) {
			return
		}

		var snap map[string]int
		res, err := store.GetSnapshot(id, &snap)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, map[string]int{"value": 1}, snap)
		assert.Equal(t, int64(1), res.NextSequence)
		if assert.Len(t, res.AdditionalEvents, 1) {
			assert.Equal(t,
				second.Type, res.AdditionalEvents[0].Type,
			)
		}
	})
}

func TestStoreAppendConflict(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "conflict")
		first := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"active", "dev", 1,
		)
		second := testEvent(t,
			time.Unix(1_700_000_001, 0).UTC(),
			"paused", "prod", 2,
		)
		stale := testEvent(t,
			time.Unix(1_700_000_002, 0).UTC(),
			"stale", "dev", 3,
		)

		if !assert.NoError(t, store.AppendEvents(
			id, 0, []*timebox.Event{first, second},
		)) {
			return
		}

		err = store.AppendEvents(
			id, 1, []*timebox.Event{stale},
		)
		if !assert.Error(t, err) {
			return
		}

		var conflict *timebox.VersionConflictError
		if !assert.ErrorAs(t, err, &conflict) {
			return
		}
		assert.Equal(t, int64(1), conflict.ExpectedSequence)
		assert.Equal(t, int64(2), conflict.ActualSequence)
		if assert.Len(t, conflict.NewEvents, 1) {
			assert.Equal(t,
				second.Type, conflict.NewEvents[0].Type,
			)
			assert.Equal(t,
				int64(1), conflict.NewEvents[0].Sequence,
			)
		}
	})
}

func TestLoadEvents(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "ev")
		first := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"a", "dev", 1,
		)
		second := testEvent(t,
			time.Unix(1_700_000_001, 0).UTC(),
			"b", "dev", 2,
		)

		if !assert.NoError(t, store.AppendEvents(
			id, 0, []*timebox.Event{first, second},
		)) {
			return
		}

		evs, err := store.GetEvents(id, 0)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, evs, 2)

		evs, err = store.GetEvents(id, 1)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, evs, 1)
	})
}

func TestListAggregates(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		a := timebox.NewAggregateID("order", "1")
		b := timebox.NewAggregateID("order", "2")
		c := timebox.NewAggregateID("user", "1")
		ev := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"active", "dev", 1,
		)
		for _, id := range []timebox.AggregateID{a, b, c} {
			if !assert.NoError(t, store.AppendEvents(
				id, 0, []*timebox.Event{ev},
			)) {
				return
			}
		}

		all, err := store.ListAggregates(nil)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, all, 3)

		orders, err := store.ListAggregates(
			timebox.NewAggregateID("order"),
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, orders, 2)

		users, err := store.ListAggregates(
			timebox.NewAggregateID("user"),
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, users, 1)
	})
}

func TestListLabelValues(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		cfg.Timebox.Indexer = statusEnvIndexer(t)

		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		a := timebox.NewAggregateID("order", "1")
		b := timebox.NewAggregateID("order", "2")
		evDev := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"active", "dev", 1,
		)
		evProd := testEvent(t,
			time.Unix(1_700_000_001, 0).UTC(),
			"active", "prod", 2,
		)
		if !assert.NoError(t,
			store.AppendEvents(a, 0, []*timebox.Event{evDev}),
		) {
			return
		}
		if !assert.NoError(t,
			store.AppendEvents(b, 0, []*timebox.Event{evProd}),
		) {
			return
		}

		vals, err := store.ListLabelValues("env")
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, []string{"dev", "prod"}, vals)
	})
}

func TestLabelRemoval(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		callCount := 0
		cfg.Timebox.Indexer = func(
			evs []*timebox.Event,
		) []*timebox.Index {
			callCount++
			res := make([]*timebox.Index, 0, len(evs))
			for range evs {
				if callCount == 1 {
					s := "active"
					res = append(res, &timebox.Index{
						Status: &s,
						Labels: map[string]string{
							"env": "dev",
						},
					})
				} else {
					s := "active"
					res = append(res, &timebox.Index{
						Status: &s,
						Labels: map[string]string{
							"env": "",
						},
					})
				}
			}
			return res
		}

		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "lbl")
		ev := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"active", "dev", 1,
		)
		if !assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{ev}),
		) {
			return
		}

		ids, err := store.ListAggregatesByLabel("env", "dev")
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, ids, 1)

		ev2 := testEvent(t,
			time.Unix(1_700_000_001, 0).UTC(),
			"active", "", 2,
		)
		if !assert.NoError(t,
			store.AppendEvents(id, 1, []*timebox.Event{ev2}),
		) {
			return
		}

		ids, err = store.ListAggregatesByLabel("env", "dev")
		if !assert.NoError(t, err) {
			return
		}
		assert.Empty(t, ids)
	})
}

func TestSnapshotStaleSequence(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "snap")
		ev := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"a", "dev", 1,
		)
		if !assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{ev}),
		) {
			return
		}
		assert.NoError(t,
			store.PutSnapshot(id, map[string]int{"v": 1}, 1),
		)
		// Stale snapshot at earlier sequence should be ignored
		assert.NoError(t,
			store.PutSnapshot(id, map[string]int{"v": 0}, 0),
		)

		var snap map[string]int
		res, err := store.GetSnapshot(id, &snap)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, map[string]int{"v": 1}, snap)
		assert.Equal(t, int64(1), res.NextSequence)
	})
}

func TestStatusForMissing(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		status, err := store.GetAggregateStatus(
			timebox.NewAggregateID("order", "missing"),
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, "", status)
	})
}

func TestAppendLabelsOnly(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		cfg.Timebox.Indexer = func(
			evs []*timebox.Event,
		) []*timebox.Index {
			res := make([]*timebox.Index, 0, len(evs))
			for range evs {
				res = append(res, &timebox.Index{
					Labels: map[string]string{
						"env": "staging",
					},
				})
			}
			return res
		}

		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "lbl-only")
		ev := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"active", "staging", 1,
		)
		if !assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{ev}),
		) {
			return
		}

		ids, err := store.ListAggregatesByLabel(
			"env", "staging",
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, []timebox.AggregateID{id}, ids)
	})
}

func TestSnapshotWithTrim(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		cfg.Timebox.Snapshot.TrimEvents = true

		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "trim")
		ev1 := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"a", "dev", 1,
		)
		ev2 := testEvent(t,
			time.Unix(1_700_000_001, 0).UTC(),
			"b", "dev", 2,
		)
		ev3 := testEvent(t,
			time.Unix(1_700_000_002, 0).UTC(),
			"c", "dev", 3,
		)
		if !assert.NoError(t, store.AppendEvents(
			id, 0, []*timebox.Event{ev1, ev2, ev3},
		)) {
			return
		}

		// Snapshot at seq 2 should trim events before 2
		assert.NoError(t,
			store.PutSnapshot(id, map[string]int{"v": 2}, 2),
		)

		evs, err := store.GetEvents(id, 0)
		if !assert.NoError(t, err) {
			return
		}
		// Events before seq 2 should be trimmed; only seq 2
		// remains
		assert.Len(t, evs, 1)
	})
}

func TestSnapshotNoTrim(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		cfg.Timebox.Snapshot.TrimEvents = false

		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "notrim")
		ev1 := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"a", "dev", 1,
		)
		ev2 := testEvent(t,
			time.Unix(1_700_000_001, 0).UTC(),
			"b", "dev", 2,
		)
		if !assert.NoError(t, store.AppendEvents(
			id, 0, []*timebox.Event{ev1, ev2},
		)) {
			return
		}

		assert.NoError(t,
			store.PutSnapshot(id, map[string]int{"v": 1}, 1),
		)

		evs, err := store.GetEvents(id, 0)
		if !assert.NoError(t, err) {
			return
		}
		// All events preserved when TrimEvents is false
		assert.Len(t, evs, 2)
	})
}

func TestSnapshotBeforeEvents(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "snap-first")

		// PutSnapshot before any events — triggers the
		// insertAggregate path in SaveSnapshot
		assert.NoError(t,
			store.PutSnapshot(id, map[string]int{"v": 0}, 0),
		)

		var snap map[string]int
		res, err := store.GetSnapshot(id, &snap)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, map[string]int{"v": 0}, snap)
		assert.Equal(t, int64(0), res.NextSequence)
		assert.Empty(t, res.AdditionalEvents)
	})
}

func TestLoadSnapshotMissing(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "ghost")
		var snap map[string]int
		res, err := store.GetSnapshot(id, &snap)
		if !assert.NoError(t, err) {
			return
		}
		assert.Nil(t, snap)
		assert.Equal(t, int64(0), res.NextSequence)
		assert.Empty(t, res.AdditionalEvents)
	})
}

func TestLoadEventsMissing(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "none")
		evs, err := store.GetEvents(id, 0)
		if !assert.NoError(t, err) {
			return
		}
		assert.Empty(t, evs)
	})
}

func TestListAggregatesEmpty(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		all, err := store.ListAggregates(nil)
		if !assert.NoError(t, err) {
			return
		}
		assert.Empty(t, all)
	})
}

func TestStatusByStatusEmpty(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		res, err := store.ListAggregatesByStatus("gone")
		if !assert.NoError(t, err) {
			return
		}
		assert.Empty(t, res)
	})
}

func TestLabelValuesEmpty(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		vals, err := store.ListLabelValues("nope")
		if !assert.NoError(t, err) {
			return
		}
		assert.Empty(t, vals)
	})
}

func TestNewStoreBadConfig(t *testing.T) {
	_, err := postgres.NewStore(postgres.Config{MaxConns: -1})
	assert.ErrorIs(t, err, postgres.ErrInvalidMaxConns)
}

func TestNewPersistenceBadConfig(t *testing.T) {
	_, err := postgres.NewPersistence(
		postgres.Config{MaxConns: -1},
	)
	assert.ErrorIs(t, err, postgres.ErrInvalidMaxConns)
}

func TestNewPersistenceBadURL(t *testing.T) {
	_, err := postgres.NewPersistence(postgres.Config{
		URL:      "postgres://localhost:1/bad?sslmode=disable",
		Prefix:   "test",
		MaxConns: 4,
	})
	assert.Error(t, err)
}

func TestNewPersistenceInvalidURL(t *testing.T) {
	_, err := postgres.NewPersistence(postgres.Config{
		URL:      "://",
		Prefix:   "test",
		MaxConns: 4,
	})
	assert.Error(t, err)
}

func TestAppendBadStatusAt(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		s := "active"
		_, err = p.Append(timebox.AppendRequest{
			ID:               timebox.NewAggregateID("o", "1"),
			ExpectedSequence: 0,
			Status:           &s,
			StatusAt:         "not-a-number",
			Events:           []string{`{}`},
		})
		assert.Error(t, err)
	})
}

func TestSaveSnapshotCancelledCtx(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		id := timebox.NewAggregateID("order", "cancel")
		err = p.SaveSnapshot(ctx, id, []byte("{}"), 0)
		assert.Error(t, err)
	})
}

func TestByLabelEmpty(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		ids, err := store.ListAggregatesByLabel("env", "x")
		if !assert.NoError(t, err) {
			return
		}
		assert.Empty(t, ids)
	})
}

func withTestDatabase(t *testing.T, fn func(context.Context, postgres.Config)) {
	t.Helper()

	ctx := context.Background()
	adminCfg, err := pgxpool.ParseConfig(postgres.DefaultURL)
	if !assert.NoError(t, err) {
		return
	}

	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer admin.Close()

	if err := admin.Ping(ctx); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}

	dbName := fmt.Sprintf(
		"timebox_test_%d", time.Now().UnixNano(),
	)
	_, err = admin.Exec(ctx,
		"CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize(),
	)
	if !assert.NoError(t, err) {
		return
	}

	defer func() {
		_, _ = admin.Exec(ctx, `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1
			  AND pid <> pg_backend_pid()
		`, dbName)
		_, _ = admin.Exec(ctx,
			"DROP DATABASE IF EXISTS "+
				pgx.Identifier{dbName}.Sanitize(),
		)
	}()

	cfg := postgres.DefaultConfig()
	cfg.URL = databaseURL(t, adminCfg.ConnString(), dbName)
	cfg.Prefix = "timebox-test"

	fn(ctx, cfg)
}

func databaseURL(t *testing.T, rawURL, dbName string) string {
	t.Helper()

	u, err := url.Parse(rawURL)
	if !assert.NoError(t, err) {
		return ""
	}
	u.Path = "/" + dbName
	return u.String()
}

func statusEnvIndexer(t *testing.T) timebox.Indexer {
	t.Helper()
	return func(evs []*timebox.Event) []*timebox.Index {
		res := make([]*timebox.Index, 0, len(evs))
		for _, ev := range evs {
			data := struct {
				Status string `json:"status"`
				Env    string `json:"env"`
			}{}
			if !assert.NoError(t,
				json.Unmarshal(ev.Data, &data),
			) {
				return nil
			}
			res = append(res, &timebox.Index{
				Status: &data.Status,
				Labels: map[string]string{
					"env": data.Env,
				},
			})
		}
		return res
	}
}

func testEvent(
	t *testing.T, at time.Time, status, env string, value int,
) *timebox.Event {
	t.Helper()

	data, err := json.Marshal(map[string]any{
		"status": status,
		"env":    env,
		"value":  value,
	})
	if !assert.NoError(t, err) {
		return nil
	}

	return &timebox.Event{
		Timestamp: at,
		Type:      "event.test",
		Data:      data,
	}
}

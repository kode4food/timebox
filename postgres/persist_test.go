package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
		if assert.Len(t, evs, 2) {
			assert.Equal(t, first.Timestamp, evs[0].Timestamp)
			assert.Equal(t, first.Type, evs[0].Type)
			assert.Equal(t, id, evs[0].AggregateID)
			assert.Equal(t, int64(0), evs[0].Sequence)
			assert.Equal(t, first.Data, evs[0].Data)
			assert.Equal(t, second.Timestamp, evs[1].Timestamp)
			assert.Equal(t, second.Type, evs[1].Type)
			assert.Equal(t, id, evs[1].AggregateID)
			assert.Equal(t, int64(1), evs[1].Sequence)
			assert.Equal(t, second.Data, evs[1].Data)
		}

		evs, err = store.GetEvents(id, 1)
		if !assert.NoError(t, err) {
			return
		}
		assert.Len(t, evs, 1)
	})
}

func TestEventRow(t *testing.T) {
	withTestDatabase(t, func(ctx context.Context, cfg postgres.Config) {
		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID("order", "row")
		ev := testEvent(t,
			time.Unix(1_700_000_000, 123).UTC(),
			"a", "dev", 1,
		)
		if !assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{ev}),
		) {
			return
		}

		pool, err := pgxpool.New(ctx, cfg.URL)
		if !assert.NoError(t, err) {
			return
		}
		defer pool.Close()

		var seq int64
		var at int64
		var typ string
		var data string
		err = pool.QueryRow(ctx, `
			SELECT sequence, event_at, event_type, data
			FROM timebox_events
			WHERE store = $1
		`, cfg.Prefix).Scan(&seq, &at, &typ, &data)
		if !assert.NoError(t, err) {
			return
		}

		var want any
		var got any
		if !assert.NoError(t, json.Unmarshal(ev.Data, &want)) {
			return
		}
		if !assert.NoError(t, json.Unmarshal([]byte(data), &got)) {
			return
		}
		assert.Equal(t, int64(0), seq)
		assert.Equal(t, ev.Timestamp.UnixNano(), at)
		assert.Equal(t, string(ev.Type), typ)
		assert.Equal(t, want, got)
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

func TestAggregateIDChars(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		cfg.Timebox.Indexer = statusEnvIndexer(t)

		store, err := postgres.NewStore(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = store.Close() }()

		id := timebox.NewAggregateID(
			`order:1`, `part;2`, `quoted["3"]`,
		)
		ev := testEvent(t,
			time.Unix(1_700_000_000, 0).UTC(),
			"active", "dev", 1,
		)
		if !assert.NoError(t,
			store.AppendEvents(id, 0, []*timebox.Event{ev}),
		) {
			return
		}

		evs, err := store.GetEvents(id, 0)
		if !assert.NoError(t, err) {
			return
		}
		if assert.Len(t, evs, 1) {
			assert.Equal(t, id, evs[0].AggregateID)
		}

		ids, err := store.ListAggregates(
			timebox.NewAggregateID(`order:1`),
		)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, []timebox.AggregateID{id}, ids)

		ids, err = store.ListAggregatesByLabel("env", "dev")
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, []timebox.AggregateID{id}, ids)
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

func TestClosedPersistence(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		assert.NoError(t, p.Close())

		id := timebox.NewAggregateID("order", "closed")

		_, err = p.LoadEvents(id, 0)
		assert.Error(t, err)

		_, err = p.LoadSnapshot(id)
		assert.Error(t, err)

		err = p.SaveSnapshot(id, []byte("{}"), 0)
		assert.Error(t, err)

		_, err = p.ListAggregates(nil)
		assert.Error(t, err)

		_, err = p.ListAggregatesByStatus("x")
		assert.Error(t, err)

		_, err = p.ListAggregatesByLabel("k", "v")
		assert.Error(t, err)

		_, err = p.ListLabelValues("k")
		assert.Error(t, err)
	})
}

func TestNewPersistenceInvalidURL(t *testing.T) {
	_, err := postgres.NewPersistence(postgres.Config{
		URL:      "://",
		Prefix:   "test",
		MaxConns: 4,
	})
	assert.Error(t, err)
}

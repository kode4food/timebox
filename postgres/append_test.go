package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

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

func TestStatusOnlyAppend(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		s := "active"
		id := timebox.NewAggregateID("order", "status-only")
		_, err = p.Append(timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: 0,
			Status:           &s,
			Events:           []string{`{}`},
		})
		assert.NoError(t, err)

		status, err := p.GetAggregateStatus(id)
		assert.NoError(t, err)
		assert.Equal(t, "active", status)
	})
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

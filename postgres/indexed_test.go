package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

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

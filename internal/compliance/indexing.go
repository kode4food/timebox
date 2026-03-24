package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func runIndexing(t *testing.T, p Profile) {
	for _, trimEvents := range []bool{false, true} {
		mode := "KeepEvents"
		if trimEvents {
			mode = "TrimEvents"
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("MissingStatus", func(t *testing.T) {
				store := openStore(t, p, StoreConfig{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})

				status, err := store.GetAggregateStatus(
					timebox.NewAggregateID("order", "missing"),
				)
				assert.NoError(t, err)
				assert.Equal(t, "", status)
			})

			t.Run("NoIndexer", func(t *testing.T) {
				store := openStore(t, p, StoreConfig{TrimEvents: trimEvents})
				id := timebox.NewAggregateID("order", "no-indexer")
				ev := testEvent(t,
					time.Unix(1_700_000_000, 0).UTC(),
					"event.test", 1, nil, nil,
				)

				assert.NoError(t,
					store.AppendEvents(id, 0, []*timebox.Event{ev}),
				)

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, "", status)

				statuses, err := store.ListAggregatesByStatus("active")
				assert.NoError(t, err)
				assert.Empty(t, statuses)

				ids, err := store.ListAggregatesByLabel("env", "prod")
				assert.NoError(t, err)
				assert.Empty(t, ids)

				vals, err := store.ListLabelValues("env")
				assert.NoError(t, err)
				assert.Empty(t, vals)
			})

			t.Run("Status", func(t *testing.T) {
				store := openStore(t, p, StoreConfig{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				first := timebox.NewAggregateID("order", "1")
				second := timebox.NewAggregateID("order", "2")
				third := timebox.NewAggregateID("order", "3")
				active := "active"
				paused := "paused"

				assert.NoError(t,
					store.AppendEvents(first, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_000, 0).UTC(),
							"event.test", 1, &active,
							map[string]string{"env": "prod"},
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(second, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_300, 0).UTC(),
							"event.test", 2, &active,
							map[string]string{"env": "stage"},
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(third, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_600, 0).UTC(),
							"event.test", 3, &paused,
							map[string]string{"env": "dev"},
						),
					}),
				)

				got, err := store.GetAggregateStatus(first)
				assert.NoError(t, err)
				assert.Equal(t, active, got)

				statuses, err := store.ListAggregatesByStatus(active)
				assert.NoError(t, err)
				assert.Equal(t, []timebox.StatusEntry{
					{
						ID:        first,
						Timestamp: time.Unix(1_700_000_000, 0).UTC(),
					},
					{
						ID:        second,
						Timestamp: time.Unix(1_700_000_300, 0).UTC(),
					},
				}, statuses)

				statuses, err = store.ListAggregatesByStatus(paused)
				assert.NoError(t, err)
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        third,
					Timestamp: time.Unix(1_700_000_600, 0).UTC(),
				}}, statuses)

				statuses, err = store.ListAggregatesByStatus("missing")
				assert.NoError(t, err)
				assert.Empty(t, statuses)
			})

			t.Run("StatusReplace", func(t *testing.T) {
				store := openStore(t, p, StoreConfig{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				id := timebox.NewAggregateID("order", "replace")
				active := "active"
				paused := "paused"

				assert.NoError(t,
					store.AppendEvents(id, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_100, 0).UTC(),
							"event.test", 1, &active, nil,
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(id, 1, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_200, 0).UTC(),
							"event.test", 2, &paused, nil,
						),
					}),
				)

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, paused, status)

				activeIDs, err := store.ListAggregatesByStatus(active)
				assert.NoError(t, err)
				assert.Empty(t, activeIDs)

				pausedIDs, err := store.ListAggregatesByStatus(paused)
				assert.NoError(t, err)
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        id,
					Timestamp: time.Unix(1_700_000_200, 0).UTC(),
				}}, pausedIDs)
			})

			t.Run("Labels", func(t *testing.T) {
				store := openStore(t, p, StoreConfig{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				first := timebox.NewAggregateID("order", "labels-1")
				second := timebox.NewAggregateID("order", "labels-2")
				active := "active"

				assert.NoError(t,
					store.AppendEvents(first, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_009, 0).UTC(),
							"event.test", 1, &active,
							map[string]string{
								"env":    "prod",
								"region": "eu",
							},
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(second, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_010, 0).UTC(),
							"event.test", 2, &active,
							map[string]string{"env": "stage"},
						),
					}),
				)

				ids, err := store.ListAggregatesByLabel("env", "prod")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{first}, ids)

				ids, err = store.ListAggregatesByLabel("region", "eu")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{first}, ids)

				vals, err := store.ListLabelValues("env")
				assert.NoError(t, err)
				assert.Equal(t, []string{"prod", "stage"}, vals)

				assert.NoError(t,
					store.AppendEvents(first, 1, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_011, 0).UTC(),
							"event.test", 3, &active,
							map[string]string{"env": ""},
						),
					}),
				)

				ids, err = store.ListAggregatesByLabel("env", "prod")
				assert.NoError(t, err)
				assert.Empty(t, ids)

				vals, err = store.ListLabelValues("env")
				assert.NoError(t, err)
				assert.Equal(t, []string{"stage"}, vals)
			})

			t.Run("LabelsOnly", func(t *testing.T) {
				store := openStore(t, p, StoreConfig{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				id := timebox.NewAggregateID("order", "labels-only")
				ev := testEvent(t,
					time.Unix(1_700_000_011, 0).UTC(),
					"event.test", 1, nil,
					map[string]string{"env": "prod"},
				)

				assert.NoError(t,
					store.AppendEvents(id, 0, []*timebox.Event{ev}),
				)

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, "", status)

				ids, err := store.ListAggregatesByLabel("env", "prod")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)

				vals, err := store.ListLabelValues("env")
				assert.NoError(t, err)
				assert.Equal(t, []string{"prod"}, vals)
			})

			t.Run("BatchIndex", func(t *testing.T) {
				store := openStore(t, p, StoreConfig{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				id := timebox.NewAggregateID("order", "batch-index")
				active := "active"
				paused := "paused"

				assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{
					testEvent(t,
						time.Unix(1_700_000_012, 0).UTC(),
						"event.test", 1, &active,
						map[string]string{"env": "prod"},
					),
					testEvent(t,
						time.Unix(1_700_000_013, 0).UTC(),
						"event.test", 2, nil,
						map[string]string{"region": "eu"},
					),
					testEvent(t,
						time.Unix(1_700_000_014, 0).UTC(),
						"event.test", 3, &paused,
						map[string]string{"env": "stage"},
					),
				}))

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, paused, status)

				statuses, err := store.ListAggregatesByStatus(paused)
				assert.NoError(t, err)
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        id,
					Timestamp: time.Unix(1_700_000_014, 0).UTC(),
				}}, statuses)

				ids, err := store.ListAggregatesByLabel("env", "stage")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)

				ids, err = store.ListAggregatesByLabel("region", "eu")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)
			})
		})
	}
}

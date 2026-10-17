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
				store := openStore(t, p, timebox.Config{
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
				store := openStore(t, p, timebox.Config{TrimEvents: trimEvents})
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

				statuses, err := store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: "active"},
				)
				assert.NoError(t, err)
				assert.Empty(t, statuses)

				ids, err := store.ListAggregatesByTag("prod")
				assert.NoError(t, err)
				assert.Empty(t, ids)
			})

			t.Run("EmptyAppendSkipsIndexer", func(t *testing.T) {
				calls := 0
				store := openStore(t, p, timebox.Config{
					TrimEvents: trimEvents,
					Indexer: func([]*timebox.Event) []*timebox.Index {
						calls++
						return nil
					},
				})
				id := timebox.NewAggregateID("order", "empty-skip")

				assert.NoError(t, store.AppendEvents(id, 0, nil))
				assert.Equal(t, 0, calls)

				assert.NoError(t,
					store.AppendEvents(id, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_008, 0).UTC(),
							"event.test", 1, nil, nil,
						),
					}),
				)
				assert.Equal(t, 1, calls)

				err := store.AppendEvents(id, 0, nil)
				assert.Error(t, err)
				assert.Equal(t, 1, calls)
			})

			t.Run("Status", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
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
							map[string]bool{"prod": true},
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(second, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_300, 0).UTC(),
							"event.test", 2, &active,
							map[string]bool{"stage": true},
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(third, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_600, 0).UTC(),
							"event.test", 3, &paused,
							map[string]bool{"dev": true},
						),
					}),
				)

				got, err := store.GetAggregateStatus(first)
				assert.NoError(t, err)
				assert.Equal(t, active, got)

				statuses, err := store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: active},
				)
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

				statuses, err = store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: paused},
				)
				assert.NoError(t, err)
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        third,
					Timestamp: time.Unix(1_700_000_600, 0).UTC(),
				}}, statuses)

				statuses, err = store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: "missing"},
				)
				assert.NoError(t, err)
				assert.Empty(t, statuses)
			})

			t.Run("StatusThrough", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				active := "active"
				at := func(sec int64) time.Time {
					return time.Unix(1_700_000_000+sec, 0).UTC()
				}
				ids := []timebox.AggregateID{
					timebox.NewAggregateID("order", "c"),
					timebox.NewAggregateID("order", "a"),
					timebox.NewAggregateID("other", "b"),
					timebox.NewAggregateID("order", "b"),
				}
				for i, id := range ids {
					assert.NoError(t,
						store.AppendEvents(id, 0, []*timebox.Event{
							testEvent(t, at(int64(i)), "event.test", i,
								&active, nil,
							),
						}),
					)
				}
				// The same status at a later time replaces the entry
				assert.NoError(t,
					store.AppendEvents(ids[0], 1, []*timebox.Event{
						testEvent(t, at(9), "event.test", 9, &active, nil),
					}),
				)

				list := func(q timebox.StatusQuery) []timebox.StatusEntry {
					entries, err := store.ListAggregatesByStatus(q)
					assert.NoError(t, err)
					return entries
				}
				assert.Equal(t, []timebox.StatusEntry{
					{ID: ids[1], Timestamp: at(1)},
					{ID: ids[3], Timestamp: at(3)},
				}, list(timebox.StatusQuery{
					Status:  active,
					Type:    "order",
					Through: at(3),
				}))
				assert.Equal(t, []timebox.StatusEntry{
					{ID: ids[1], Timestamp: at(1)},
					{ID: ids[2], Timestamp: at(2)},
				}, list(timebox.StatusQuery{
					Status:  active,
					Through: at(2),
				}))
				assert.Equal(t, []timebox.StatusEntry{
					{ID: ids[1], Timestamp: at(1)},
					{ID: ids[3], Timestamp: at(3)},
					{ID: ids[0], Timestamp: at(9)},
				}, list(timebox.StatusQuery{Status: active, Type: "order"}))
			})

			t.Run("StatusKeyPrefix", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				active := "active"
				ids := []timebox.AggregateID{
					timebox.NewAggregateID("order", "abcd1"),
					timebox.NewAggregateID("order", "abcd2"),
					timebox.NewAggregateID("order", "abce1"),
					timebox.NewAggregateID("order", "ab"),
					timebox.NewAggregateID("other", "abcd9"),
				}
				for i, id := range ids {
					assert.NoError(t,
						store.AppendEvents(id, 0, []*timebox.Event{
							testEvent(t,
								time.Unix(1_700_000_000+int64(i), 0).UTC(),
								"event.test", i, &active, nil,
							),
						}),
					)
				}

				for _, tc := range []struct {
					prefix timebox.ID
					want   []timebox.AggregateID
				}{
					{"", ids[:4]},
					{"a", ids[:4]},
					{"abc", ids[:3]},
					{"abcd", ids[:2]},
					{"abcd1", ids[:1]},
					{"abd", nil},
				} {
					entries, err := store.ListAggregatesByStatus(
						timebox.StatusQuery{
							Status:    active,
							Type:      "order",
							KeyPrefix: tc.prefix,
						},
					)
					assert.NoError(t, err)
					var got []timebox.AggregateID
					for _, entry := range entries {
						got = append(got, entry.ID)
					}
					assert.Equal(t, tc.want, got)
				}
			})

			t.Run("StatusReplace", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
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

				activeIDs, err := store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: active},
				)
				assert.NoError(t, err)
				assert.Empty(t, activeIDs)

				pausedIDs, err := store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: paused},
				)
				assert.NoError(t, err)
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        id,
					Timestamp: time.Unix(1_700_000_200, 0).UTC(),
				}}, pausedIDs)
			})

			t.Run("StatusClear", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				id := timebox.NewAggregateID("order", "clear")
				active := "active"
				assert.NoError(t,
					store.AppendEvents(id, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_201, 0).UTC(),
							"event.test", 1, &active,
							map[string]bool{"prod": true},
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(id, 1, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_202, 0).UTC(),
							"event.test", 2, nil,
							map[string]bool{"prod": false},
						),
						testEvent(t,
							time.Unix(1_700_000_203, 0).UTC(),
							"event.test", 3, new(""), nil,
						),
					}),
				)

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Empty(t, status)

				statuses, err := store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: active},
				)
				assert.NoError(t, err)
				assert.Empty(t, statuses)

				ids, err := store.ListAggregatesByTag("prod")
				assert.NoError(t, err)
				assert.Empty(t, ids)
			})

			t.Run("Tags", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				first := timebox.NewAggregateID("order", "tags-1")
				second := timebox.NewAggregateID("order", "tags-2")
				active := "active"

				assert.NoError(t,
					store.AppendEvents(first, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_009, 0).UTC(),
							"event.test", 1, &active,
							map[string]bool{
								"prod":   true,
								"eu":     true,
								"shared": true,
							},
						),
					}),
				)
				assert.NoError(t,
					store.AppendEvents(second, 0, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_010, 0).UTC(),
							"event.test", 2, &active,
							map[string]bool{"stage": true, "shared": true},
						),
					}),
				)

				ids, err := store.ListAggregatesByTag("prod")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{first}, ids)

				ids, err = store.ListAggregatesByTag("eu")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{first}, ids)

				ids, err = store.ListAggregatesByTag("shared")
				assert.NoError(t, err)
				assert.ElementsMatch(t,
					[]timebox.AggregateID{first, second}, ids,
				)

				assert.NoError(t,
					store.AppendEvents(first, 1, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_011, 0).UTC(),
							"event.test", 3, &active,
							map[string]bool{"prod": false, "shared": false},
						),
					}),
				)

				ids, err = store.ListAggregatesByTag("prod")
				assert.NoError(t, err)
				assert.Empty(t, ids)

				ids, err = store.ListAggregatesByTag("shared")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{second}, ids)
			})

			t.Run("TagsOnly", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				id := timebox.NewAggregateID("order", "tags-only")
				ev := testEvent(t,
					time.Unix(1_700_000_011, 0).UTC(),
					"event.test", 1, nil,
					map[string]bool{"prod": true},
				)

				assert.NoError(t,
					store.AppendEvents(id, 0, []*timebox.Event{ev}),
				)

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, "", status)

				ids, err := store.ListAggregatesByTag("prod")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)
			})

			t.Run("BatchIndex", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				id := timebox.NewAggregateID("order", "batch-index")
				paused := "paused"

				assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{
					testEvent(t,
						time.Unix(1_700_000_012, 0).UTC(),
						"event.test", 1, new("active"),
						map[string]bool{"prod": true},
					),
					testEvent(t,
						time.Unix(1_700_000_013, 0).UTC(),
						"event.test", 2, nil,
						map[string]bool{"eu": true},
					),
					testEvent(t,
						time.Unix(1_700_000_014, 0).UTC(),
						"event.test", 3, &paused,
						map[string]bool{"prod": false, "stage": true},
					),
				}))

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, paused, status)

				statuses, err := store.ListAggregatesByStatus(
					timebox.StatusQuery{Status: paused},
				)
				assert.NoError(t, err)
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        id,
					Timestamp: time.Unix(1_700_000_014, 0).UTC(),
				}}, statuses)

				ids, err := store.ListAggregatesByTag("stage")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)

				ids, err = store.ListAggregatesByTag("eu")
				assert.NoError(t, err)
				assert.ElementsMatch(t, []timebox.AggregateID{id}, ids)
			})
		})
	}
}

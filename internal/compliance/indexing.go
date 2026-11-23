package compliance

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

const testPageSize = 100

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

				status, err := store.GetAggregateStatus(timebox.NewAggregateID("order", "missing"))
				assert.NoError(t, err)
				assert.Equal(t, "", status)
			})

			t.Run("NoIndexer", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{TrimEvents: trimEvents})
				id := timebox.NewAggregateID("order", "no-indexer")
				ev := testEvent(t, time.Unix(1_700_000_000, 0).UTC(), "event.test", 1, nil, nil)

				assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, "", status)
				assert.Empty(t, listStatus(t, store, "active", "order"))
				assert.Empty(t, tagsOf(t, store, id))
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
						testEvent(t, time.Unix(1_700_000_008, 0).UTC(), "event.test", 1, nil, nil),
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

				assert.Equal(t, []timebox.StatusEntry{
					{ID: second, Timestamp: time.Unix(1_700_000_300, 0).UTC()},
					{ID: first, Timestamp: time.Unix(1_700_000_000, 0).UTC()},
				}, listStatus(t, store, active, "order"))
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        third,
					Timestamp: time.Unix(1_700_000_600, 0).UTC(),
				}}, listStatus(t, store, paused, "order"))
				assert.Empty(t, listStatus(t, store, "missing", "order"))
			})

			t.Run("StatusPaging", func(t *testing.T) {
				store := openStore(t, p, timebox.Config{
					Indexer:    newIndexer(t),
					TrimEvents: trimEvents,
				})
				active := "active"
				at := func(sec int64) time.Time {
					return time.Unix(1_700_000_000+sec, 0).UTC()
				}
				entry := func(typ, key timebox.ID, sec int64) timebox.StatusEntry {
					return timebox.StatusEntry{
						ID:        timebox.NewAggregateID(typ, key),
						Timestamp: at(sec),
					}
				}
				k1 := entry("order", "k1", 1)
				k2 := entry("order", "k2", 1)
				k3 := entry("order", "k3", 2)
				k4 := entry("order", "k4", 3)
				k5 := entry("order", "k5", 3)
				o1 := entry("other", "o1", 2)
				for i, e := range []timebox.StatusEntry{k5, o1, k2, k4, k1, k3} {
					assert.NoError(t,
						store.AppendEvents(e.ID, 0, []*timebox.Event{
							testEvent(t, e.Timestamp, "event.test", i, &active, nil),
						}),
					)
				}
				// The same status at a later time moves the entry
				assert.NoError(t,
					store.AppendEvents(k1.ID, 1, []*timebox.Event{
						testEvent(t, at(9), "event.test", 9, &active, nil),
					}),
				)
				k1 = entry("order", "k1", 9)

				list := func(q timebox.StatusQuery) []timebox.StatusEntry {
					q.Status = active
					entries, err := store.ListAggregatesByStatus(q)
					assert.NoError(t, err)
					return entries
				}

				t.Run("Pages", func(t *testing.T) {
					q := timebox.StatusQuery{
						Type:  "order",
						Limit: 2,
					}
					assert.Equal(t, []timebox.StatusEntry{k1, k5}, list(q))
					q.After = &k5
					assert.Equal(t, []timebox.StatusEntry{k4, k3}, list(q))
					q.After = &k3
					assert.Equal(t, []timebox.StatusEntry{k2}, list(q))
				})

				t.Run("Through", func(t *testing.T) {
					assert.Equal(t, []timebox.StatusEntry{k3, k2}, list(timebox.StatusQuery{
						Type:    "order",
						Through: at(2),
						Limit:   2,
					}))
				})

				t.Run("OtherType", func(t *testing.T) {
					assert.Equal(t, []timebox.StatusEntry{o1}, list(timebox.StatusQuery{
						Type:  "other",
						Limit: testPageSize,
					}))
				})

				t.Run("InvalidLimit", func(t *testing.T) {
					_, err := store.ListAggregatesByStatus(timebox.StatusQuery{
						Status: active,
						Type:   "order",
					})
					assert.ErrorIs(t, err, timebox.ErrInvalidLimit)
				})
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
				assert.Empty(t, listStatus(t, store, active, "order"))
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        id,
					Timestamp: time.Unix(1_700_000_200, 0).UTC(),
				}}, listStatus(t, store, paused, "order"))
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
				assert.Empty(t, listStatus(t, store, active, "order"))
				assert.Empty(t, tagsOf(t, store, id))
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

				assert.Equal(t, []string{"eu", "prod", "shared"}, tagsOf(t, store, first))
				assert.Equal(t, []string{"shared", "stage"}, tagsOf(t, store, second))

				both, err := store.GetAggregateTags(first, second)
				assert.NoError(t, err)
				assert.Len(t, both, 2)

				assert.NoError(t,
					store.AppendEvents(first, 1, []*timebox.Event{
						testEvent(t,
							time.Unix(1_700_000_011, 0).UTC(),
							"event.test", 3, &active,
							map[string]bool{"prod": false, "shared": false},
						),
					}),
				)

				assert.Equal(t, []string{"eu"}, tagsOf(t, store, first))
				assert.Empty(t, tagsOf(t, store, timebox.NewAggregateID("order", "none")))
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

				assert.NoError(t, store.AppendEvents(id, 0, []*timebox.Event{ev}))

				status, err := store.GetAggregateStatus(id)
				assert.NoError(t, err)
				assert.Equal(t, "", status)
				assert.Equal(t, []string{"prod"}, tagsOf(t, store, id))
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
				assert.Equal(t, []timebox.StatusEntry{{
					ID:        id,
					Timestamp: time.Unix(1_700_000_014, 0).UTC(),
				}}, listStatus(t, store, paused, "order"))
				assert.Equal(t, []string{"eu", "stage"}, tagsOf(t, store, id))
			})
		})
	}
}

func listStatus(
	t *testing.T, store *timebox.Store, status string, typ timebox.ID,
) []timebox.StatusEntry {
	t.Helper()
	entries, err := store.ListAggregatesByStatus(timebox.StatusQuery{
		Status: status,
		Type:   typ,
		Limit:  testPageSize,
	})
	assert.NoError(t, err)
	return entries
}

func tagsOf(t *testing.T, store *timebox.Store, id timebox.AggregateID) []string {
	t.Helper()
	tags, err := store.GetAggregateTags(id)
	assert.NoError(t, err)
	return slices.Sorted(slices.Values(tags[id]))
}

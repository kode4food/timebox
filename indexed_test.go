package timebox_test

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestStoreGetAggregateStatus(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("Missing", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "status-current-missing", combinedIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						_ *redis.Client,
					) {
						status, err := store.GetAggregateStatus(ctx,
							timebox.NewAggregateID("order", "1"))
						assert.NoError(t, err)
						assert.Equal(t, "", status)
					},
				)
			})

			t.Run("CurrentStatus", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "status-current", combinedIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						_ *redis.Client,
					) {
						id := timebox.NewAggregateID("order", "1")

						assert.NoError(t,
							store.AppendEvents(ctx, id, 0, []*timebox.Event{
								combinedEvent(id, "active", "prod"),
							}),
						)
						assert.NoError(t,
							store.AppendEvents(ctx, id, 1, []*timebox.Event{
								combinedEvent(id, "paused", "stage"),
							}),
						)

						status, err := store.GetAggregateStatus(ctx, id)
						assert.NoError(t, err)
						assert.Equal(t, "paused", status)
					},
				)
			})
		})
	}
}

func TestStoreStatusHelpers(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("ListAggregatesByStatus", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-list", func(
					ctx context.Context, store *timebox.Store, _ *redis.Client,
				) {
					active := "active"
					paused := "paused"
					active1 := timebox.NewAggregateID("order", "1")
					active2 := timebox.NewAggregateID("order", "2")
					paused1 := timebox.NewAggregateID("order", "3")

					assert.NoError(t,
						store.AppendEvents(ctx, active1, 0, []*timebox.Event{
							statusEventAt(active1, &active,
								time.Unix(1700000000, 0).UTC(),
							),
						}),
					)
					assert.NoError(t,
						store.AppendEvents(ctx, active2, 0, []*timebox.Event{
							statusEventAt(active2, &active,
								time.Unix(1700000300, 0).UTC()),
						}),
					)
					assert.NoError(t,
						store.AppendEvents(ctx, paused1, 0, []*timebox.Event{
							statusEventAt(paused1, &paused,
								time.Unix(1700000600, 0).UTC()),
						}),
					)

					activeEntries, err := store.ListAggregatesByStatus(ctx,
						active)
					assert.NoError(t, err)
					assert.ElementsMatch(t, []timebox.StatusEntry{
						{
							ID:        active1,
							Timestamp: time.Unix(1700000000, 0).UTC(),
						},
						{
							ID:        active2,
							Timestamp: time.Unix(1700000300, 0).UTC(),
						},
					}, activeEntries)

					pausedEntries, err := store.ListAggregatesByStatus(ctx,
						paused)
					assert.NoError(t, err)
					assert.Equal(t, []timebox.StatusEntry{{
						ID:        paused1,
						Timestamp: time.Unix(1700000600, 0).UTC(),
					}}, pausedEntries)

					missingEntries, err := store.ListAggregatesByStatus(
						ctx, "missing",
					)
					assert.NoError(t, err)
					assert.Empty(t, missingEntries)
				})
			})
		})
	}
}

func TestStoreStatusIndexing(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("NoIndexNoMutation", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-no-index", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")

					err := store.AppendEvents(
						ctx, id, 0, []*timebox.Event{statusEvent(id, nil)},
					)
					assert.NoError(t, err)

					raw, err := client.LIndex(
						ctx, eventListKey("status-no-index", id), 0,
					).Result()
					assert.NoError(t, err)
					assert.NotContains(t, raw, `"index"`)

					assertNoCurrentStatus(t, ctx, client, "status-no-index", id)
					assertStatusMembership(
						t, ctx, client, "status-no-index", "active", id, false,
					)

					events, err := store.GetEvents(ctx, id, 0)
					assert.NoError(t, err)
					assert.Len(t, events, 1)
				})
			})

			t.Run("SingleStatusSetsMembership", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-single", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					ts := time.Unix(1700000000, 0).UTC()

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEventAt(id, &active, ts),
					})
					assert.NoError(t, err)

					raw, err := client.LIndex(
						ctx, eventListKey("status-single", id), 0,
					).Result()
					assert.NoError(t, err)
					assert.NotContains(t, raw, `"index"`)

					assertCurrentStatus(
						t, ctx, client, "status-single", id, active,
					)
					assertStatusTimestamp(
						t, ctx, client, "status-single", active, id, ts,
					)
					assertStatusMembership(
						t, ctx, client, "status-single", active, id, true,
					)

					events, err := store.GetEvents(ctx, id, 0)
					assert.NoError(t, err)
					assert.Len(t, events, 1)
				})
			})

			t.Run("FinalStatusWinsWithinBatch", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-batch", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					pending := "pending"
					processing := "processing"
					active := "active"

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &pending),
						statusEvent(id, &processing),
						statusEvent(id, &active),
					})
					assert.NoError(t, err)

					assertCurrentStatus(
						t, ctx, client, "status-batch", id, active,
					)
					assertStatusMembership(
						t, ctx, client, "status-batch", pending, id, false,
					)
					assertStatusMembership(
						t, ctx, client, "status-batch", processing, id, false,
					)
					assertStatusMembership(
						t, ctx, client, "status-batch", active, id, true,
					)
				})
			})

			t.Run("TransitionMovesBetweenSets", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-transition", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					paused := "paused"
					activeTS := time.Unix(1700000000, 0).UTC()
					pausedTS := activeTS.Add(5 * time.Minute)

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEventAt(id, &active, activeTS),
					})
					assert.NoError(t, err)

					err = store.AppendEvents(ctx, id, 1, []*timebox.Event{
						statusEventAt(id, &paused, pausedTS),
					})
					assert.NoError(t, err)

					assertCurrentStatus(
						t, ctx, client, "status-transition", id, paused,
					)
					assertStatusTimestamp(t, ctx, client,
						"status-transition", paused, id, pausedTS)
					assertStatusMembership(
						t, ctx, client, "status-transition", active, id, false,
					)
					assertStatusMembership(
						t, ctx, client, "status-transition", paused, id, true,
					)
				})
			})

			t.Run("ClearStatusRemovesMembership", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-clear", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					cleared := ""
					activeTS := time.Unix(1700000000, 0).UTC()

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEventAt(id, &active, activeTS),
					})
					assert.NoError(t, err)

					err = store.AppendEvents(ctx, id, 1, []*timebox.Event{
						statusEvent(id, &cleared),
					})
					assert.NoError(t, err)

					assertNoCurrentStatus(t, ctx, client, "status-clear", id)
					assertNoStatusTimestamp(
						t, ctx, client, "status-clear", active, id,
					)
					assertStatusMembership(
						t, ctx, client, "status-clear", active, id, false,
					)
				})
			})

			t.Run("SameStatusPreservesOriginalTimestamp", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-same", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					firstTS := time.Unix(1700000000, 0).UTC()
					secondTS := firstTS.Add(10 * time.Minute)

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEventAt(id, &active, firstTS),
					})
					assert.NoError(t, err)

					err = store.AppendEvents(ctx, id, 1, []*timebox.Event{
						statusEventAt(id, &active, secondTS),
					})
					assert.NoError(t, err)

					assertCurrentStatus(
						t, ctx, client, "status-same", id, active,
					)
					assertStatusTimestamp(t, ctx, client, "status-same",
						active, id, firstTS)
				})
			})

			t.Run("ConflictDoesNotMutateStatus", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-conflict", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")
					active := "active"
					paused := "paused"

					err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &active),
					})
					assert.NoError(t, err)

					err = store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &paused),
					})
					assert.Error(t, err)

					assertCurrentStatus(
						t, ctx, client, "status-conflict", id, active,
					)
					assertStatusMembership(
						t, ctx, client, "status-conflict", active, id, true,
					)
					assertStatusMembership(
						t, ctx, client, "status-conflict", paused, id, false,
					)
				})
			})
		})
	}
}

func TestStoreLabelHelpers(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		mode := "untrimmed"
		if trimEvents {
			mode = "trimmed"
		}

		t.Run(mode, func(t *testing.T) {
			t.Run("ListAggregatesByLabel", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "labels-list", labelIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						client *redis.Client,
					) {
						id := timebox.NewAggregateID("order", "1")

						assert.NoError(t,
							store.AppendEvents(ctx, id, 0, []*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "prod"},
								),
							}),
						)

						assertLabelMembership(t,
							ctx, client, "labels-list", "env", "prod", id,
							true,
						)

						ids, err := store.ListAggregatesByLabel(
							ctx, "env", "prod",
						)
						assert.NoError(t, err)
						assert.Equal(t, []timebox.AggregateID{id}, ids)
					},
				)
			})

			t.Run("ListLabelValues", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "labels-values", labelIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						client *redis.Client,
					) {
						id := timebox.NewAggregateID("order", "1")

						assert.NoError(t,
							store.AppendEvents(ctx, id, 0, []*timebox.Event{
								labelEvent(id, map[string]string{
									"env":    "prod",
									"region": "eu",
								}),
							}),
						)
						assert.NoError(t,
							store.AppendEvents(ctx, id, 1, []*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "stage"},
								),
							}),
						)

						assertLabelMembership(t,
							ctx, client, "labels-values", "env", "prod", id,
							false,
						)
						assertLabelMembership(t,
							ctx, client, "labels-values", "env", "stage", id,
							true,
						)

						vals, err := store.ListLabelValues(ctx, "env")
						assert.NoError(t, err)
						assert.Equal(t, []string{"stage"}, vals)
					},
				)
			})

			t.Run("FinalValueWinsWithinBatch", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "labels-batch", labelIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						client *redis.Client,
					) {
						id := timebox.NewAggregateID("order", "1")

						assert.NoError(t,
							store.AppendEvents(ctx, id, 0, []*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "dev"},
								),
								labelEvent(id,
									map[string]string{"env": "prod"},
								),
							}),
						)

						assertLabelMembership(t,
							ctx, client, "labels-batch", "env", "dev", id,
							false,
						)
						assertLabelMembership(t,
							ctx, client, "labels-batch", "env", "prod", id,
							true,
						)
					},
				)
			})

			t.Run("EmptyValueRemovesMembership", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "labels-empty", labelIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						client *redis.Client,
					) {
						id := timebox.NewAggregateID("order", "1")

						assert.NoError(t,
							store.AppendEvents(ctx, id, 0, []*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "prod"},
								),
							}),
						)
						assert.NoError(t,
							store.AppendEvents(ctx, id, 1, []*timebox.Event{
								labelEvent(id,
									map[string]string{"env": ""}),
							}),
						)

						assertLabelMembership(t,
							ctx, client, "labels-empty", "env", "prod", id,
							false,
						)
						ids, err := store.ListAggregatesByLabel(ctx, "env", "")
						assert.NoError(t, err)
						assert.Empty(t, ids)

						vals, err := store.ListLabelValues(ctx, "env")
						assert.NoError(t, err)
						assert.Empty(t, vals)
					},
				)
			})

			t.Run("ConflictDoesNotMutateLabels", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "labels-conflict", labelIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						client *redis.Client,
					) {
						id := timebox.NewAggregateID("order", "1")

						assert.NoError(t, store.AppendEvents(ctx, id, 0,
							[]*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "prod"},
								),
							},
						))

						err := store.AppendEvents(ctx, id, 0,
							[]*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "stage"},
								),
							},
						)
						assert.Error(t, err)

						assertLabelMembership(t,
							ctx, client, "labels-conflict", "env", "prod", id,
							true,
						)
						assertLabelMembership(t,
							ctx, client, "labels-conflict", "env", "stage", id,
							false,
						)
					},
				)
			})
		})
	}
}

func statusEvent(id timebox.AggregateID, status *string) *timebox.Event {
	return statusEventAt(id, status, time.Now())
}

func statusEventAt(
	id timebox.AggregateID, status *string, ts time.Time,
) *timebox.Event {
	data := json.RawMessage(`1`)
	if status != nil {
		data = json.RawMessage(strconv.Quote(*status))
	}

	return &timebox.Event{
		Timestamp:   ts,
		Type:        EventIncremented,
		AggregateID: id,
		Data:        data,
	}
}

func statusIndexer(events []*timebox.Event) []*timebox.Index {
	var res []*timebox.Index
	for _, ev := range events {
		var status string
		if err := json.Unmarshal(ev.Data, &status); err == nil {
			res = append(res, &timebox.Index{Status: &status})
		}
	}
	return res
}

func labelEvent(id timebox.AggregateID, lbls map[string]string) *timebox.Event {
	data, err := json.Marshal(lbls)
	if err != nil {
		panic(err)
	}

	return &timebox.Event{
		Timestamp:   time.Now(),
		Type:        EventIncremented,
		AggregateID: id,
		Data:        data,
	}
}

func labelIndexer(events []*timebox.Event) []*timebox.Index {
	var res []*timebox.Index
	for _, ev := range events {
		lbls := map[string]string{}
		if err := json.Unmarshal(ev.Data, &lbls); err == nil {
			res = append(res, &timebox.Index{Labels: lbls})
		}
	}
	return res
}

func currentStatusKey(prefix string) string {
	return prefix + ":idx:status"
}

func statusIndexKey(prefix, status string) string {
	return prefix + ":idx:status:" + status
}

func labelIndexKey(prefix, label, value string) string {
	return prefix + ":idx:label:" + escapeKeyPart(label) + ":" +
		escapeKeyPart(value)
}

func assertCurrentStatus(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	id timebox.AggregateID, expected string,
) {
	t.Helper()

	actual, err := client.HGet(
		ctx, currentStatusKey(prefix), id.Join(":"),
	).Result()
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func assertNoCurrentStatus(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	id timebox.AggregateID,
) {
	t.Helper()

	_, err := client.HGet(ctx, currentStatusKey(prefix), id.Join(":")).Result()
	assert.ErrorIs(t, err, redis.Nil)
}

func assertStatusTimestamp(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	status string, id timebox.AggregateID, expected time.Time,
) {
	t.Helper()

	actual, err := client.ZScore(
		ctx, statusIndexKey(prefix, status), id.Join(":"),
	).Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(expected.UnixMilli()), actual)
}

func assertNoStatusTimestamp(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	status string, id timebox.AggregateID,
) {
	t.Helper()

	_, err := client.ZScore(
		ctx, statusIndexKey(prefix, status), id.Join(":"),
	).Result()
	assert.ErrorIs(t, err, redis.Nil)
}

func assertStatusMembership(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	status string, id timebox.AggregateID, expected bool,
) {
	t.Helper()

	if expected {
		_, err := client.ZScore(
			ctx, statusIndexKey(prefix, status), id.Join(":"),
		).Result()
		assert.NoError(t, err)
		return
	}
	_, err := client.ZScore(
		ctx, statusIndexKey(prefix, status), id.Join(":"),
	).Result()
	assert.ErrorIs(t, err, redis.Nil)
}

func assertLabelMembership(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	label string, value string, id timebox.AggregateID, expected bool,
) {
	t.Helper()

	exists, err := client.SIsMember(
		ctx, labelIndexKey(prefix, label, value), id.Join(":"),
	).Result()
	assert.NoError(t, err)
	assert.Equal(t, expected, exists)
}

func escapeKeyPart(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	return strings.ReplaceAll(s, ":", `%`)
}

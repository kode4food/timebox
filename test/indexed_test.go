package integration_test

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
						status, err := store.GetAggregateStatus(
							timebox.NewAggregateID("order", "1"),
						)
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
							store.AppendEvents(id, 0, []*timebox.Event{
								combinedEvent(id, "active", "prod"),
							}),
						)
						assert.NoError(t,
							store.AppendEvents(id, 1, []*timebox.Event{
								combinedEvent(id, "paused", "stage"),
							}),
						)

						status, err := store.GetAggregateStatus(id)
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
					first := timebox.NewAggregateID("order", "1")
					active := "active"
					assert.NoError(t,
						store.AppendEvents(first, 0, []*timebox.Event{
							statusEventAt(first, &active,
								time.Unix(1700000000, 0).UTC(),
							),
						}),
					)

					second := timebox.NewAggregateID("order", "2")
					assert.NoError(t,
						store.AppendEvents(second, 0, []*timebox.Event{
							statusEventAt(second, &active,
								time.Unix(1700000300, 0).UTC()),
						}),
					)

					paused := "paused"
					third := timebox.NewAggregateID("order", "3")
					assert.NoError(t,
						store.AppendEvents(third, 0, []*timebox.Event{
							statusEventAt(third, &paused,
								time.Unix(1700000600, 0).UTC()),
						}),
					)

					got, err := store.ListAggregatesByStatus(active)
					assert.NoError(t, err)
					assert.ElementsMatch(t, []timebox.StatusEntry{
						{
							ID:        first,
							Timestamp: time.Unix(1700000000, 0).UTC(),
						},
						{
							ID:        second,
							Timestamp: time.Unix(1700000300, 0).UTC(),
						},
					}, got)

					got, err = store.ListAggregatesByStatus(paused)
					assert.NoError(t, err)
					assert.Equal(t, []timebox.StatusEntry{{
						ID:        third,
						Timestamp: time.Unix(1700000600, 0).UTC(),
					}}, got)

					got, err = store.ListAggregatesByStatus("missing")
					assert.NoError(t, err)
					assert.Empty(t, got)
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
						id, 0, []*timebox.Event{statusEvent(id, nil)},
					)
					assert.NoError(t, err)

					raw, err := client.LIndex(ctx,
						eventListKey("status-no-index", id), 0,
					).Result()
					assert.NoError(t, err)
					assert.NotContains(t, raw, `"index"`)

					assertNoCurrentStatus(t, ctx, client, "status-no-index", id)
					assertStatusMembership(
						t, ctx, client, "status-no-index", "active", id, false,
					)

					events, err := store.GetEvents(id, 0)
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

					err := store.AppendEvents(id, 0, []*timebox.Event{
						statusEventAt(id, &active, ts),
					})
					assert.NoError(t, err)

					raw, err := client.LIndex(ctx,
						eventListKey("status-single", id), 0,
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

					events, err := store.GetEvents(id, 0)
					assert.NoError(t, err)
					assert.Len(t, events, 1)
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
							store.AppendEvents(id, 0, []*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "prod"},
								),
							}),
						)

						assertLabelMembership(t, ctx,
							client, "labels-list", "env", "prod", id, true,
						)

						ids, err := store.ListAggregatesByLabel("env", "prod")
						assert.NoError(t, err)
						assert.Equal(t, []timebox.AggregateID{id}, ids)
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
		Type:        eventIncremented,
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
		Type:        eventIncremented,
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

	actual, err := client.HGet(ctx,
		currentStatusKey(prefix), id.Join(":"),
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

	actual, err := client.ZScore(ctx,
		statusIndexKey(prefix, status), id.Join(":"),
	).Result()
	assert.NoError(t, err)
	assert.Equal(t, float64(expected.UnixMilli()), actual)
}

func assertStatusMembership(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	status string, id timebox.AggregateID, expected bool,
) {
	t.Helper()

	if expected {
		_, err := client.ZScore(ctx,
			statusIndexKey(prefix, status), id.Join(":"),
		).Result()
		assert.NoError(t, err)
		return
	}

	_, err := client.ZScore(ctx,
		statusIndexKey(prefix, status), id.Join(":"),
	).Result()
	assert.ErrorIs(t, err, redis.Nil)
}

func assertLabelMembership(
	t *testing.T, ctx context.Context, client *redis.Client, prefix string,
	label string, value string, id timebox.AggregateID, expected bool,
) {
	t.Helper()

	exists, err := client.SIsMember(ctx,
		labelIndexKey(prefix, label, value), id.Join(":"),
	).Result()
	assert.NoError(t, err)
	assert.Equal(t, expected, exists)
}

func escapeKeyPart(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	return strings.ReplaceAll(s, ":", `%`)
}

package timebox_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

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
							true,
						)
						assertLabelMembership(t,
							ctx, client, "labels-values", "env", "stage", id,
							true,
						)

						vals, err := store.ListLabelValues(ctx, "env")
						assert.NoError(t, err)
						assert.Equal(t, []string{"prod", "stage"}, vals)
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

			t.Run("EmptyValueIsIgnored", func(t *testing.T) {
				withIndexedStore(t,
					trimEvents, "labels-empty", labelIndexer,
					func(
						ctx context.Context, store *timebox.Store,
						_ *redis.Client,
					) {
						id := timebox.NewAggregateID("order", "1")

						assert.NoError(t,
							store.AppendEvents(ctx, id, 0, []*timebox.Event{
								labelEvent(id, map[string]string{"env": ""}),
							}),
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

						assert.NoError(t,
							store.AppendEvents(ctx, id, 0, []*timebox.Event{
								labelEvent(id,
									map[string]string{"env": "prod"},
								),
							}),
						)

						err := store.AppendEvents(ctx, id, 0, []*timebox.Event{
							labelEvent(id, map[string]string{"env": "stage"}),
						})
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
	res := []*timebox.Index{}
	for _, ev := range events {
		lbls := map[string]string{}
		if err := json.Unmarshal(ev.Data, &lbls); err == nil {
			res = append(res, &timebox.Index{Labels: lbls})
		}
	}
	return res
}

func labelIndexKey(prefix, label, value string) string {
	return prefix + ":labels:" + escapeKeyPart(label) + ":" +
		escapeKeyPart(value)
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

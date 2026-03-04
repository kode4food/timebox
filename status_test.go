package timebox_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

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

			t.Run("RemoveAggregateFromStatusClearsCurrentStatus",
				func(t *testing.T) {
					withStatusStore(t,
						trimEvents, "status-remove-match", func(
							ctx context.Context, store *timebox.Store,
							client *redis.Client,
						) {
							active := "active"
							id := timebox.NewAggregateID("order", "1")

							assert.NoError(t,
								store.AppendEvents(ctx, id, 0, []*timebox.Event{
									statusEvent(id, &active),
								}),
							)

							assert.NoError(t,
								store.RemoveAggregateFromStatus(
									ctx, id, active,
								),
							)

							entries, err := store.ListAggregatesByStatus(ctx,
								active)
							assert.NoError(t, err)
							assert.Empty(t, entries)

							assertNoCurrentStatus(t,
								ctx, client, "status-remove-match", id,
							)
							assertNoStatusTimestamp(t,
								ctx, client, "status-remove-match", active, id,
							)
							assertStatusMembership(t,
								ctx, client, "status-remove-match", active,
								id, false,
							)
						},
					)
				},
			)

			t.Run("RemoveAggregateFromStatusLeavesOtherCurrentStatus",
				func(t *testing.T) {
					withStatusStore(t,
						trimEvents, "status-remove-mismatch", func(
							ctx context.Context, store *timebox.Store,
							client *redis.Client,
						) {
							active := "active"
							paused := "paused"
							id := timebox.NewAggregateID("order", "1")
							activeTS := time.Unix(1700000000, 0).UTC()
							pausedTS := activeTS.Add(time.Minute)

							assert.NoError(t,
								store.AppendEvents(
									ctx, id, 0, []*timebox.Event{
										statusEventAt(id, &active, activeTS),
									},
								),
							)
							assert.NoError(t,
								store.AppendEvents(
									ctx, id, 1, []*timebox.Event{
										statusEventAt(id, &paused, pausedTS),
									},
								),
							)

							err := client.ZAdd(
								ctx,
								statusIndexKey("status-remove-mismatch",
									active),
								redis.Z{
									Member: id.Join(":"),
									Score:  float64(activeTS.UnixMilli()),
								},
							).Err()
							assert.NoError(t, err)

							assert.NoError(t,
								store.RemoveAggregateFromStatus(
									ctx, id, active,
								),
							)

							assertCurrentStatus(t,
								ctx, client, "status-remove-mismatch",
								id, paused,
							)
							assertStatusTimestamp(t, ctx, client,
								"status-remove-mismatch", paused, id,
								pausedTS)
							assertStatusMembership(t,
								ctx, client, "status-remove-mismatch",
								active, id, false,
							)
							assertStatusMembership(t,
								ctx, client, "status-remove-mismatch",
								paused, id, true,
							)

							entries, err := store.ListAggregatesByStatus(ctx,
								paused)
							assert.NoError(t, err)
							assert.Equal(t, []timebox.StatusEntry{{
								ID:        id,
								Timestamp: pausedTS,
							}}, entries)
						},
					)
				},
			)

			t.Run("RemoveAggregateFromMissingStatusIsNoOp", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-remove-missing", func(
					ctx context.Context, store *timebox.Store,
					client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")

					assert.NoError(t,
						store.RemoveAggregateFromStatus(ctx, id, "missing"),
					)
					assertNoCurrentStatus(
						t, ctx, client, "status-remove-missing", id,
					)
					assertNoStatusTimestamp(
						t, ctx, client, "status-remove-missing", "missing", id,
					)
				})
			})
		})
	}
}

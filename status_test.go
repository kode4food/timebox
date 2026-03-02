package timebox_test

import (
	"context"
	"testing"

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

					assert.NoError(t, store.AppendEvents(ctx, active1, 0, []*timebox.Event{
						statusEvent(active1, &timebox.Index{Status: &active}),
					}))
					assert.NoError(t, store.AppendEvents(ctx, active2, 0, []*timebox.Event{
						statusEvent(active2, &timebox.Index{Status: &active}),
					}))
					assert.NoError(t, store.AppendEvents(ctx, paused1, 0, []*timebox.Event{
						statusEvent(paused1, &timebox.Index{Status: &paused}),
					}))

					activeIDs, err := store.ListAggregatesByStatus(ctx, active)
					assert.NoError(t, err)
					assert.ElementsMatch(t,
						[]timebox.AggregateID{active1, active2},
						activeIDs,
					)

					pausedIDs, err := store.ListAggregatesByStatus(ctx, paused)
					assert.NoError(t, err)
					assert.Equal(t, []timebox.AggregateID{paused1}, pausedIDs)

					missingIDs, err := store.ListAggregatesByStatus(ctx, "missing")
					assert.NoError(t, err)
					assert.Empty(t, missingIDs)
				})
			})

			t.Run("RemoveAggregateFromStatusClearsCurrentStatus", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-remove-match", func(
					ctx context.Context, store *timebox.Store, client *redis.Client,
				) {
					active := "active"
					id := timebox.NewAggregateID("order", "1")

					assert.NoError(t, store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &timebox.Index{Status: &active}),
					}))

					assert.NoError(t, store.RemoveAggregateFromStatus(ctx, id, active))

					ids, err := store.ListAggregatesByStatus(ctx, active)
					assert.NoError(t, err)
					assert.Empty(t, ids)

					assertNoCurrentStatus(t, ctx, client, "status-remove-match", id)
					assertStatusMembership(
						t, ctx, client, "status-remove-match", active, id, false,
					)
				})
			})

			t.Run("RemoveAggregateFromStatusLeavesOtherCurrentStatus", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-remove-mismatch", func(
					ctx context.Context, store *timebox.Store, client *redis.Client,
				) {
					active := "active"
					paused := "paused"
					id := timebox.NewAggregateID("order", "1")

					assert.NoError(t, store.AppendEvents(ctx, id, 0, []*timebox.Event{
						statusEvent(id, &timebox.Index{Status: &active}),
					}))
					assert.NoError(t, store.AppendEvents(ctx, id, 1, []*timebox.Event{
						statusEvent(id, &timebox.Index{Status: &paused}),
					}))

					err := client.SAdd(
						ctx, statusSetKey("status-remove-mismatch", active), id.Join(":"),
					).Err()
					assert.NoError(t, err)

					assert.NoError(t, store.RemoveAggregateFromStatus(ctx, id, active))

					assertCurrentStatus(
						t, ctx, client, "status-remove-mismatch", id, paused,
					)
					assertStatusMembership(
						t, ctx, client, "status-remove-mismatch", active, id, false,
					)
					assertStatusMembership(
						t, ctx, client, "status-remove-mismatch", paused, id, true,
					)

					ids, err := store.ListAggregatesByStatus(ctx, paused)
					assert.NoError(t, err)
					assert.Equal(t, []timebox.AggregateID{id}, ids)
				})
			})

			t.Run("RemoveAggregateFromMissingStatusIsNoOp", func(t *testing.T) {
				withStatusStore(t, trimEvents, "status-remove-missing", func(
					ctx context.Context, store *timebox.Store, client *redis.Client,
				) {
					id := timebox.NewAggregateID("order", "1")

					assert.NoError(t,
						store.RemoveAggregateFromStatus(ctx, id, "missing"),
					)
					assertNoCurrentStatus(t, ctx, client, "status-remove-missing", id)
				})
			})
		})
	}
}

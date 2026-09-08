package compliance

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

// counter accumulates the payload testEvent already encodes, so a
// transaction's and a competing writer's events share one shape
type counter struct {
	Value int `json:"value"`
}

var counterAppliers = timebox.Appliers[counter]{
	"counted": timebox.MakeApplier(
		func(st counter, _ *timebox.Event, data indexData) counter {
			st.Value += data.Value
			return st
		},
	),
}

func runTransactions(t *testing.T, p Profile) {
	t.Run("Concurrent", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		exec := store.Executor(newCounter, counterAppliers)
		ids := make([]timebox.AggregateID, 24)
		for i := range ids {
			ids[i] = timebox.NewAggregateID("tx", timebox.ID(strconv.Itoa(i)))
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				<-start
				err := store.Transact(func(tx *timebox.Transaction) error {
					for _, id := range ids {
						if _, err := tx.Exec(exec, id, count(1)); err != nil {
							return err
						}
					}
					return nil
				})
				assert.NoError(t, err)
			})
		}
		close(start)
		wg.Wait()
		for _, id := range ids {
			st, err := exec.Get(id)
			assert.NoError(t, err)
			assert.Equal(t, 8, st.Value)
		}
	})

	t.Run("Atomic", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		exec := store.Executor(newCounter, counterAppliers)

		first := timebox.NewAggregateID("tx", "first")
		second := timebox.NewAggregateID("tx", "second")

		err := store.Transact(func(tx *timebox.Transaction) error {
			if _, err := tx.Exec(exec, first, count(1)); err != nil {
				return err
			}
			_, err := tx.Exec(exec, second, count(2))
			return err
		})
		assert.NoError(t, err)

		assertEvents(t, store, first, 1)
		assertEvents(t, store, second, 1)
	})

	t.Run("DuplicateAggregate", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{})
		id := timebox.NewAggregateID("tx", "duplicate")
		req := timebox.AppendRequest{
			ID: id,
			Events: []*timebox.Event{
				testEvent(t,
					time.Unix(1_700_000_010, 0).UTC(),
					"counted", 1, nil, nil,
				),
			},
		}

		err := backend.Append(req, req)
		assert.ErrorIs(t, err, timebox.ErrDuplicateAggregate)
		assertEvents(t, store, id, 0)
	})

	t.Run("RollsBack", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		exec := store.Executor(newCounter, counterAppliers)

		contended := timebox.NewAggregateID("tx", "contended")
		partner := timebox.NewAggregateID("tx", "partner")

		attempts := 0
		err := store.Transact(func(tx *timebox.Transaction) error {
			attempts++
			if _, err := tx.Exec(exec, contended, count(1)); err != nil {
				return err
			}
			if attempts == 1 {
				// a competing writer lands between the load and the commit
				err := store.AppendEvents(contended, 0, []*timebox.Event{
					testEvent(t,
						time.Unix(1_700_000_009, 0).UTC(),
						"counted", 9, nil, nil,
					),
				})
				if err != nil {
					return err
				}
			}
			_, err := tx.Exec(exec, partner, count(2))
			return err
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, attempts)

		// the conflicting attempt staged an append for partner too. Had the
		// commit not rolled back, partner would hold two events
		assertEvents(t, store, partner, 1)
		assertEvents(t, store, contended, 2)
	})
}

func newCounter() counter {
	return counter{}
}

func count(by int) timebox.Command[counter] {
	return func(_ counter, ag *timebox.Aggregator[counter]) error {
		return ag.Raise("counted", indexData{Value: by})
	}
}

func assertEvents(
	t *testing.T, store *timebox.Store, id timebox.AggregateID, n int,
) []*timebox.Event {
	t.Helper()

	evs, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, evs, n)
	return evs
}

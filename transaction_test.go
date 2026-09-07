package timebox_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

// otherState gives one Store a second aggregate state type
type otherState struct{}

func TestTransactionCommitsBothAggregates(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	child := timebox.NewAggregateID("counter", "child")
	parent := timebox.NewAggregateID("counter", "parent")

	var childVal, parentVal int
	err := store.Transaction(func(tx *timebox.Transaction) error {
		st, err := tx.Exec(executor, child, raiseCount(3))
		if err != nil {
			return err
		}
		childVal = st.Value

		// the parent's command consumes the child's optimistic result
		st, err = tx.Exec(executor, parent, raiseCount(st.Value))
		parentVal = st.Value
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, childVal)
	assert.Equal(t, 3, parentVal)

	assertEventCount(t, store, child, 1)
	assertEventCount(t, store, parent, 1)
}

func TestTransactionRollsBackOnConflict(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	contended := timebox.NewAggregateID("counter", "contended")
	partner := timebox.NewAggregateID("counter", "partner")

	attempts := 0
	err := store.Transaction(func(tx *timebox.Transaction) error {
		attempts++
		if _, err := tx.Exec(executor, contended, raiseCount(1)); err != nil {
			return err
		}
		if attempts == 1 {
			// a competing writer lands between the load and the commit
			if err := store.AppendEvents(contended, 0, []*timebox.Event{
				{Type: EventIncremented, Data: []byte("9")},
			}); err != nil {
				return err
			}
		}
		_, err := tx.Exec(executor, partner, raiseCount(1))
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, attempts)

	// the first attempt staged an append for partner and conflicted on
	// contended. Had it not rolled back, partner would hold two events
	assertEventCount(t, store, partner, 1)
	assertEventCount(t, store, contended, 2)
}

func TestTransactionDiscardsOnError(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "discarded")
	boom := errors.New("boom")

	err := store.Transaction(func(tx *timebox.Transaction) error {
		if _, err := tx.Exec(executor, id, raiseCount(1)); err != nil {
			return err
		}
		return boom
	})
	assert.ErrorIs(t, err, boom)
	assertEventCount(t, store, id, 0)
}

func TestTransactionJoinsSameAggregateTwice(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = store.Close() }()
	defer func() { _ = server.Close() }()

	id := timebox.NewAggregateID("counter", "twice")

	var final int
	err := store.Transaction(func(tx *timebox.Transaction) error {
		if _, err := tx.Exec(executor, id, raiseCount(2)); err != nil {
			return err
		}
		st, err := tx.Exec(executor, id, raiseCount(5))
		final = st.Value
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, 7, final)

	// both batches merged into one append, sequenced from zero
	events := assertEventCount(t, store, id, 2)
	assert.Equal(t, int64(0), events[0].Sequence)
	assert.Equal(t, int64(1), events[1].Sequence)

	st, err := executor.Get(id)
	assert.NoError(t, err)
	assert.Equal(t, 7, st.Value)
}

func TestTransactionSuccessActionsRunAfterCommit(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	id := timebox.NewAggregateID("counter", "success")
	ran := 0

	err := store.Transaction(func(tx *timebox.Transaction) error {
		_, err := tx.Exec(executor, id,
			func(_ CounterState, ag *timebox.Aggregator[CounterState]) error {
				ag.OnSuccess(func(CounterState, []*timebox.Event) {
					ran++
				})
				assert.Equal(t, 0, ran)
				return ag.Raise(EventIncremented, 1)
			},
		)
		return err
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, ran)
}

func TestTransactionRejectsForeignExecutor(t *testing.T) {
	server, store, _ := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	other, otherStore, otherExec := setupTestExecutor(t)
	defer func() { _ = other.Close() }()
	defer func() { _ = otherStore.Close() }()

	err := store.Transaction(func(tx *timebox.Transaction) error {
		_, err := tx.Exec(otherExec,
			timebox.NewAggregateID("counter", "foreign"), raiseCount(1),
		)
		return err
	})
	assert.ErrorIs(t, err, timebox.ErrStoreMismatch)
}

func raiseCount(by int) timebox.Command[CounterState] {
	return func(_ CounterState, ag *timebox.Aggregator[CounterState]) error {
		return ag.Raise(EventIncremented, by)
	}
}

func assertEventCount(
	t *testing.T, store *timebox.Store, id timebox.AggregateID, count int,
) []*timebox.Event {
	t.Helper()

	events, err := store.GetEvents(id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, count)
	return events
}

func TestTransactionRejectsConflictingStateTypes(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	// a second executor over the same Store with a different state type
	other := store.Executor(
		func() otherState { return otherState{} },
		timebox.Appliers[otherState]{},
	)
	id := timebox.NewAggregateID("counter", "conflicting")

	err := store.Transaction(func(tx *timebox.Transaction) error {
		if _, err := tx.Exec(executor, id, raiseCount(1)); err != nil {
			return err
		}
		_, err := tx.Exec(other, id,
			func(_ otherState, _ *timebox.Aggregator[otherState]) error {
				return nil
			},
		)
		return err
	})
	assert.ErrorIs(t, err, timebox.ErrAggregateTypeConflict)
	assertEventCount(t, store, id, 0)
}

func TestAggregatorTransaction(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	first := timebox.NewAggregateID("counter", "holder")
	second := timebox.NewAggregateID("counter", "enlisted")

	var seen *timebox.Transaction
	err := store.Transaction(func(tx *timebox.Transaction) error {
		_, err := tx.Exec(executor, first,
			func(_ CounterState, ag *timebox.Aggregator[CounterState]) error {
				seen = ag.Transaction()
				assert.Same(t, tx, seen)
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
				// only the Aggregator is in scope, yet a second aggregate
				// still joins this same commit
				_, err := seen.Exec(executor, second, raiseCount(2))
				return err
			},
		)
		return err
	})
	assert.NoError(t, err)

	assertEventCount(t, store, first, 1)
	assertEventCount(t, store, second, 1)

	st, err := executor.Get(second)
	assert.NoError(t, err)
	assert.Equal(t, 2, st.Value)
}

// TestAggregatorTransactionRollsBack proves an aggregate enlisted through the
// Aggregator is a real participant, not a separate commit
func TestAggregatorTransactionRollsBack(t *testing.T) {
	server, store, executor := setupTestExecutor(t)
	defer func() { _ = server.Close() }()
	defer func() { _ = store.Close() }()

	holder := timebox.NewAggregateID("counter", "holder")
	enlisted := timebox.NewAggregateID("counter", "enlisted")
	boom := errors.New("boom")

	err := store.Transaction(func(tx *timebox.Transaction) error {
		if _, err := tx.Exec(executor, holder,
			func(_ CounterState, ag *timebox.Aggregator[CounterState]) error {
				if err := ag.Raise(EventIncremented, 1); err != nil {
					return err
				}
				_, err := ag.Transaction().Exec(executor, enlisted, raiseCount(2))
				return err
			},
		); err != nil {
			return err
		}
		return boom
	})
	assert.ErrorIs(t, err, boom)

	assertEventCount(t, store, holder, 0)
	assertEventCount(t, store, enlisted, 0)
}

package compliance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

type (
	queueData struct {
		Name string `json:"name"`
	}

	queueValidationCase struct {
		name    string
		message *timebox.Message
		err     error
	}
)

var (
	errQueueTest = errors.New("queue test error")
)

func runQueues(t *testing.T, p Profile) {
	t.Run("Order", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		for _, name := range []string{"first", "second", "third"} {
			assert.NoError(t, store.Transact(
				func(tx *timebox.Transaction) error {
					return tx.Publish("jobs", queueMessage(t, name))
				},
			))
		}

		var names []string
		for range 3 {
			err := store.Consume(t.Context(), "jobs",
				func(_ *timebox.Transaction, msg *timebox.Message) error {
					data, err := msg.GetValue[queueData]()
					if err != nil {
						return err
					}
					names = append(names, data.Name)
					return nil
				},
			)
			assert.NoError(t, err)
		}
		assert.Equal(t, []string{"first", "second", "third"}, names)
	})

	t.Run("Replay", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{}, nil)
		assert.NoError(t, store.Transact(
			func(tx *timebox.Transaction) error {
				return tx.Publish("jobs", queueMessage(t, "replay"))
			},
		))

		reloaded, err := timebox.NewStore(backend)
		assert.NoError(t, err)
		var name string
		err = reloaded.Consume(t.Context(), "jobs",
			func(_ *timebox.Transaction, msg *timebox.Message) error {
				data, err := msg.GetValue[queueData]()
				name = data.Name
				return err
			},
		)
		assert.NoError(t, err)
		assert.Equal(t, "replay", name)
	})

	t.Run("HandlerError", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		assert.NoError(t, store.Transact(
			func(tx *timebox.Transaction) error {
				return tx.Publish("jobs", queueMessage(t, "retained"))
			},
		))

		err := store.Consume(t.Context(), "jobs",
			func(*timebox.Transaction, *timebox.Message) error {
				return errQueueTest
			},
		)
		assert.ErrorIs(t, err, errQueueTest)

		var name string
		err = store.Consume(t.Context(), "jobs",
			func(_ *timebox.Transaction, msg *timebox.Message) error {
				data, err := msg.GetValue[queueData]()
				name = data.Name
				return err
			},
		)
		assert.NoError(t, err)
		assert.Equal(t, "retained", name)
	})

	t.Run("Compose", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		exec := store.Executor(newCounter, counterAppliers.Reduce)
		target := timebox.NewAggregateID("queue-target", "compose")
		assert.NoError(t, store.Transact(
			func(tx *timebox.Transaction) error {
				return tx.Publish("jobs", queueMessage(t, "compose"))
			},
		))

		err := store.Consume(t.Context(), "jobs",
			func(tx *timebox.Transaction, _ *timebox.Message) error {
				_, err := tx.Exec(exec, target, count(2))
				return err
			},
		)
		assert.NoError(t, err)

		state, err := exec.Get(target)
		assert.NoError(t, err)
		assert.Equal(t, 2, state.Value)
	})

	t.Run("Concurrent", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		exec := store.Executor(newCounter, counterAppliers.Reduce)
		target := timebox.NewAggregateID("queue-target", "concurrent")
		assert.NoError(t, store.Transact(
			func(tx *timebox.Transaction) error {
				return tx.Publish("jobs", queueMessage(t, "concurrent"))
			},
		))

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		start := make(chan struct{})
		errs := make(chan error, 2)
		for range 2 {
			go func() {
				<-start
				errs <- store.Consume(ctx, "jobs",
					func(tx *timebox.Transaction, _ *timebox.Message) error {
						_, err := tx.Exec(exec, target, count(1))
						return err
					},
				)
			}()
		}
		close(start)
		first := <-errs
		cancel()
		second := <-errs
		assert.NoError(t, first)
		assert.ErrorIs(t, second, context.Canceled)

		state, err := exec.Get(target)
		assert.NoError(t, err)
		assert.Equal(t, 1, state.Value)
	})

	t.Run("Empty", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		called := false
		err := store.Consume(ctx, "jobs",
			func(*timebox.Transaction, *timebox.Message) error {
				called = true
				return nil
			},
		)
		assert.ErrorIs(t, err, context.Canceled)
		assert.False(t, called)
	})

	t.Run("Invalid", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		cases := []queueValidationCase{
			{name: "Message", err: timebox.ErrQueueMessageRequired},
			{
				name: "Type",
				message: &timebox.Message{
					AggregateID: timebox.NewAggregateID("target", "type"),
				},
				err: timebox.ErrQueueMessageTypeRequired,
			},
			{
				name:    "ID",
				message: &timebox.Message{Type: "queue.test"},
				err:     timebox.ErrInvalidAggregateID,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := store.Transact(func(tx *timebox.Transaction) error {
					return tx.Publish("jobs", tc.message)
				})
				assert.ErrorIs(t, err, tc.err)
			})
		}
		assert.ErrorIs(t,
			store.Consume(t.Context(), "jobs", nil),
			timebox.ErrQueueHandlerRequired,
		)
	})
}

func queueMessage(t *testing.T, name string) *timebox.Message {
	t.Helper()
	data, err := json.Marshal(queueData{Name: name})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return &timebox.Message{
		Type:        "queue.test",
		AggregateID: timebox.NewAggregateID("queue-target", timebox.ID(name)),
		Data:        data,
	}
}

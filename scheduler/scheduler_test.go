package scheduler_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
	"github.com/kode4food/timebox/scheduler"
)

type (
	payload struct {
		Value string
	}

	configCase struct {
		name string
		cfg  scheduler.Config
		err  error
	}

	testTimer struct {
		ch     chan time.Time
		delays chan time.Duration
	}

	failOnceBackend struct {
		timebox.Backend
		failed atomic.Bool
	}
)

func TestRecovery(t *testing.T) {
	store := newStore(t)
	scheduleEvent(t, store, "recover", time.Now().Add(-time.Second))
	emitted := make(chan *timebox.Message, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: 10 * time.Millisecond,
		RetryDelay:     10 * time.Millisecond,
		Emitter: func(
			_ context.Context, delivery *scheduler.Delivery,
		) error {
			err := store.Transact(func(tx *timebox.Transaction) error {
				return delivery.Consume(tx)
			})
			if err == nil {
				emitted <- delivery.Message()
			}
			return err
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	var got *timebox.Message
	ok := assert.Eventually(t,
		func() bool {
			select {
			case got = <-emitted:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond,
	)
	if ok && assert.NotNil(t, got) {
		data, err := got.GetValue[payload]()
		assert.NoError(t, err)
		assert.Equal(t, payload{Value: "recover"}, data)
	}
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)

	remaining, err := store.LoadSchedule("recover")
	assert.NoError(t, err)
	assert.Nil(t, remaining)
}

func TestReconcileRetry(t *testing.T) {
	base := memory.Open()
	t.Cleanup(func() { assert.NoError(t, base.Close()) })
	backend := &failOnceBackend{Backend: base}
	store, err := timebox.NewStore(backend)
	assert.NoError(t, err)
	scheduleEvent(
		t, store, "retry-reconcile", time.Now().Add(-time.Second),
	)
	<-store.ScheduleChanges()
	emitted := make(chan struct{}, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:      store,
		RetryDelay: time.Millisecond,
		Emitter: func(
			_ context.Context, delivery *scheduler.Delivery,
		) error {
			err := store.Transact(
				func(tx *timebox.Transaction) error {
					return delivery.Consume(tx)
				},
			)
			if err == nil {
				emitted <- struct{}{}
			}
			return err
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	assert.Eventually(t,
		func() bool {
			select {
			case <-emitted:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond,
	)
	assert.True(t, backend.failed.Load())
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestEarlier(t *testing.T) {
	store := newStore(t)
	later := time.Now().Add(time.Hour)
	scheduleEvent(t, store, "later", later)
	scheduleEvent(t, store, "same-time", later)
	emitted := make(chan string, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: time.Hour,
		RetryDelay:     10 * time.Millisecond,
		Emitter: func(
			_ context.Context, delivery *scheduler.Delivery,
		) error {
			err := store.Transact(func(tx *timebox.Transaction) error {
				return delivery.Consume(tx)
			})
			if err == nil {
				msg := delivery.Message()
				data, dataErr := msg.GetValue[payload]()
				if dataErr != nil {
					return dataErr
				}
				emitted <- data.Value
			}
			return err
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	scheduleEvent(t, store, "earlier", time.Now().Add(20*time.Millisecond))
	var key string
	assert.Eventually(t,
		func() bool {
			select {
			case key = <-emitted:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond,
	)
	assert.Equal(t, "earlier", key)
}

func TestRetry(t *testing.T) {
	store := newStore(t)
	scheduleEvent(t, store, "retry", time.Now().Add(-time.Second))
	var attempts atomic.Int32
	emitted := make(chan struct{}, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: time.Hour,
		RetryDelay:     10 * time.Millisecond,
		Emitter: func(
			_ context.Context, delivery *scheduler.Delivery,
		) error {
			if attempts.Add(1) == 1 {
				return assert.AnError
			}
			err := store.Transact(func(tx *timebox.Transaction) error {
				return delivery.Consume(tx)
			})
			if err == nil {
				emitted <- struct{}{}
			}
			return err
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	assert.Eventually(t,
		func() bool {
			select {
			case <-emitted:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond,
	)
	assert.Equal(t, int32(2), attempts.Load())
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestReschedule(t *testing.T) {
	store := newStore(t)
	scheduleEvent(t, store, "repeat", time.Now().Add(-time.Second))
	var attempts atomic.Int32
	emitted := make(chan struct{}, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: time.Hour,
		RetryDelay:     10 * time.Millisecond,
		Emitter: func(
			_ context.Context, delivery *scheduler.Delivery,
		) error {
			attempt := attempts.Add(1)
			err := store.Transact(func(tx *timebox.Transaction) error {
				if err := delivery.Consume(tx); err != nil {
					return err
				}
				if attempt == 1 {
					at := time.Now().Add(20 * time.Millisecond)
					return tx.Schedule("repeat", at, delivery.Message())
				}
				return nil
			})
			if err == nil && attempt == 2 {
				emitted <- struct{}{}
			}
			return err
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	assert.Eventually(t,
		func() bool {
			select {
			case <-emitted:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond,
	)
	assert.Equal(t, int32(2), attempts.Load())
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestCancel(t *testing.T) {
	store := newStore(t)
	scheduleEvent(t, store, "cancel", time.Now().Add(40*time.Millisecond))
	emitted := make(chan struct{}, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: time.Hour,
		Emitter: func(context.Context, *scheduler.Delivery) error {
			emitted <- struct{}{}
			return nil
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	assert.NoError(t,
		store.Transact(func(tx *timebox.Transaction) error {
			return tx.CancelSchedule("cancel")
		}),
	)
	assert.Never(t,
		func() bool {
			select {
			case <-emitted:
				return true
			default:
				return false
			}
		}, 80*time.Millisecond, time.Millisecond,
	)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestCancelStopsDueEmissions(t *testing.T) {
	store := newStore(t)
	now := time.Now().Add(-time.Second)
	scheduleEvent(t, store, "first", now)
	scheduleEvent(t, store, "second", now)

	ctx, cancel := context.WithCancel(t.Context())
	var emitted atomic.Int32
	runner, err := scheduler.New(scheduler.Config{
		Store: store,
		Emitter: func(context.Context, *scheduler.Delivery) error {
			emitted.Add(1)
			cancel()
			return nil
		},
	})
	assert.NoError(t, err)

	assert.ErrorIs(t, runner.Run(ctx), context.Canceled)
	assert.Equal(t, int32(1), emitted.Load())
}

func TestReplace(t *testing.T) {
	store, peer := newStores(t)
	scheduleEvent(t, store, "replace", time.Now().Add(time.Hour))
	emitted := make(chan *timebox.Message, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: time.Hour,
		Emitter: func(
			_ context.Context, delivery *scheduler.Delivery,
		) error {
			emitted <- delivery.Message()
			return nil
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	assert.NoError(t, peer.Transact(func(tx *timebox.Transaction) error {
		return tx.Schedule(
			"replace", time.Now().Add(20*time.Millisecond),
			newMessage(t, "replacement"),
		)
	}))
	runner.Wake()
	var got *timebox.Message
	ok := assert.Eventually(t,
		func() bool {
			select {
			case got = <-emitted:
				return true
			default:
				return false
			}
		}, time.Second, time.Millisecond,
	)
	if ok && assert.NotNil(t, got) {
		data, err := got.GetValue[payload]()
		assert.NoError(t, err)
		assert.Equal(t, payload{Value: "replacement"}, data)
	}
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestConfig(t *testing.T) {
	store := newStore(t)
	emit := func(context.Context, *scheduler.Delivery) error { return nil }
	cases := []configCase{
		{
			name: "Store",
			cfg:  scheduler.Config{Emitter: emit},
			err:  scheduler.ErrStoreRequired,
		},
		{
			name: "Emitter",
			cfg:  scheduler.Config{Store: store},
			err:  scheduler.ErrEmitterRequired,
		},
		{
			name: "Rescan",
			cfg: scheduler.Config{
				Store: store, Emitter: emit, RescanInterval: -1,
			},
			err: scheduler.ErrInvalidRescanInterval,
		},
		{
			name: "Retry",
			cfg: scheduler.Config{
				Store: store, Emitter: emit, RetryDelay: -1,
			},
			err: scheduler.ErrInvalidRetryDelay,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scheduler.New(tc.cfg)
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

func TestTimer(t *testing.T) {
	store := newStore(t)
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	scheduleEvent(t, store, "timer", now.Add(time.Hour))
	delays := make(chan time.Duration, 1)
	timer := &testTimer{
		ch:     make(chan time.Time),
		delays: delays,
	}
	runner, err := scheduler.New(scheduler.Config{
		Store: store,
		Emitter: func(
			context.Context, *scheduler.Delivery,
		) error {
			return nil
		},
		Clock: func() time.Time { return now },
		TimerConstructor: func(delay time.Duration) scheduler.Timer {
			delays <- delay
			return timer
		},
	})
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	assert.Zero(t, <-delays)
	assert.Equal(t, scheduler.DefaultRescanInterval, <-delays)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func (t *testTimer) Channel() <-chan time.Time {
	return t.ch
}

func (t *testTimer) Reset(delay time.Duration) bool {
	if t.delays != nil {
		t.delays <- delay
	}
	return true
}

func (*testTimer) Stop() bool {
	return true
}

func (b *failOnceBackend) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	if !b.failed.Swap(true) {
		return nil, assert.AnError
	}
	return b.Backend.ListAggregatesByStatus(status)
}

func newStore(t *testing.T) *timebox.Store {
	t.Helper()
	store, _ := newStores(t)
	return store
}

func newStores(t *testing.T) (*timebox.Store, *timebox.Store) {
	t.Helper()
	backend := memory.Open()
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })
	first, err := backend.NewStore()
	assert.NoError(t, err)
	second, err := backend.NewStore()
	assert.NoError(t, err)
	return first, second
}

func scheduleEvent(
	t *testing.T, store *timebox.Store, key timebox.ScheduleKey, at time.Time,
) {
	t.Helper()
	assert.NoError(t,
		store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule(key, at, newMessage(t, string(key)))
		}),
	)
}

func newMessage(t *testing.T, value string) *timebox.Message {
	t.Helper()
	data, err := json.Marshal(payload{Value: value})
	if !assert.NoError(t, err) {
		return nil
	}
	return &timebox.Message{
		AggregateID: timebox.NewAggregateID(
			"schedule-target", timebox.ID(value),
		),
		Type: "scheduler.test",
		Data: data,
	}
}

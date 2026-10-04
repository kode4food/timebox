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
)

func TestRecovery(t *testing.T) {
	store := newStore(t)
	scheduleEvent(t, store, "recover", time.Now().Add(-time.Second))
	emitted := make(chan *timebox.Schedule, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: 10 * time.Millisecond,
		RetryDelay:     10 * time.Millisecond,
		Emitter: func(_ context.Context, schedule *timebox.Schedule) error {
			err := store.Transact(func(tx *timebox.Transaction) error {
				return tx.ConsumeSchedule(schedule.Key, schedule.Version)
			})
			if err == nil {
				emitted <- schedule
			}
			return err
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	var got *timebox.Schedule
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
		assert.Equal(t, timebox.ScheduleKey("recover"), got.Key)
	}
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)

	remaining, err := store.LoadSchedule("recover")
	assert.NoError(t, err)
	assert.Nil(t, remaining)
}

func TestEarlier(t *testing.T) {
	store := newStore(t)
	later := time.Now().Add(time.Hour)
	scheduleEvent(t, store, "later", later)
	scheduleEvent(t, store, "same-time", later)
	emitted := make(chan timebox.ScheduleKey, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: time.Hour,
		RetryDelay:     10 * time.Millisecond,
		Emitter: func(_ context.Context, schedule *timebox.Schedule) error {
			err := store.Transact(func(tx *timebox.Transaction) error {
				return tx.ConsumeSchedule(schedule.Key, schedule.Version)
			})
			if err == nil {
				emitted <- schedule.Key
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
	runner.Wake()
	var key timebox.ScheduleKey
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
	assert.Equal(t, timebox.ScheduleKey("earlier"), key)
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
		Emitter: func(_ context.Context, schedule *timebox.Schedule) error {
			if attempts.Add(1) == 1 {
				return assert.AnError
			}
			err := store.Transact(func(tx *timebox.Transaction) error {
				return tx.ConsumeSchedule(schedule.Key, schedule.Version)
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
		Emitter: func(_ context.Context, schedule *timebox.Schedule) error {
			attempt := attempts.Add(1)
			err := store.Transact(func(tx *timebox.Transaction) error {
				if err := tx.ConsumeSchedule(
					schedule.Key, schedule.Version,
				); err != nil {
					return err
				}
				if attempt == 1 {
					return tx.Schedule(
						schedule.Key, time.Now().Add(20*time.Millisecond),
						schedule.Event,
					)
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
		Emitter: func(context.Context, *timebox.Schedule) error {
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
	runner.Wake()
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
		Emitter: func(context.Context, *timebox.Schedule) error {
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
	emitted := make(chan *timebox.Schedule, 1)
	runner, err := scheduler.New(scheduler.Config{
		Store:          store,
		RescanInterval: time.Hour,
		Emitter: func(_ context.Context, schedule *timebox.Schedule) error {
			emitted <- schedule
			return nil
		},
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	time.Sleep(10 * time.Millisecond)
	scheduleEvent(t, peer, "replace", time.Now().Add(20*time.Millisecond))
	want := loadSchedule(t, peer, "replace")
	runner.Wake()
	var got *timebox.Schedule
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
		assert.Equal(t, want.Version, got.Version)
	}
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

func TestConfig(t *testing.T) {
	store := newStore(t)
	emit := func(context.Context, *timebox.Schedule) error { return nil }
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
		Store:   store,
		Emitter: func(context.Context, *timebox.Schedule) error { return nil },
		Clock:   func() time.Time { return now },
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

func loadSchedule(
	t *testing.T, store *timebox.Store, key timebox.ScheduleKey,
) *timebox.Schedule {
	t.Helper()
	schedule, err := store.LoadSchedule(key)
	if !assert.NoError(t, err) || !assert.NotNil(t, schedule) {
		return &timebox.Schedule{Event: &timebox.Event{}}
	}
	return schedule
}

func scheduleEvent(
	t *testing.T, store *timebox.Store, key timebox.ScheduleKey, at time.Time,
) {
	t.Helper()
	data, err := json.Marshal(payload{Value: string(key)})
	if !assert.NoError(t, err) {
		return
	}
	event := &timebox.Event{
		AggregateID: timebox.NewAggregateID("schedule-target", timebox.ID(key)),
		Type:        "scheduler.test",
		Data:        data,
	}
	assert.NoError(t,
		store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule(key, at, event)
		}),
	)
}

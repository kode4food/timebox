package compliance

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

type (
	scheduleData struct {
		Value int
	}

	scheduleItem struct {
		key timebox.ScheduleKey
		at  time.Time
	}

	scheduleState struct {
		Value int
	}

	scheduleSnapshot struct {
		Schedules   timebox.Schedules       `json:"schedules"`
		NextVersion timebox.ScheduleVersion `json:"next_version"`
	}
)

const (
	scheduleChanged timebox.EventType = "schedule.changed"
	testScheduler   timebox.ID        = "test"
)

var scheduleAppliers = timebox.Appliers[scheduleState]{
	scheduleChanged: timebox.MakeApplier(
		func(st scheduleState, _ *timebox.Event, value int) scheduleState {
			st.Value += value
			return st
		},
	),
}

func runSchedules(t *testing.T, p Profile) {
	t.Run("Lifecycle", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		at := time.Now().UTC().Add(time.Hour)
		msg := newScheduleMessage(t, "one")

		err := store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule(testScheduler, "one", at, msg)
		})
		assert.NoError(t, err)

		first := loadSchedule(t, store, "one")
		assert.Equal(t, timebox.ScheduleVersion(0), first.Version)

		err = store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule(testScheduler, "one", at.Add(time.Hour), msg)
		})
		assert.NoError(t, err)
		second := loadSchedule(t, store, "one")
		assert.Greater(t, second.Version, first.Version)

		err = store.Transact(func(tx *timebox.Transaction) error {
			return tx.ConsumeSchedule(testScheduler, second.Key, second.Version)
		})
		assert.NoError(t, err)
		consumed, err := store.LoadSchedule(testScheduler, "one")
		assert.NoError(t, err)
		assert.Nil(t, consumed)
	})

	t.Run("Named", func(t *testing.T) {
		backend, store := openBackend(t, p, timebox.Config{}, nil)
		first := timebox.ID("first")
		second := timebox.ID("second")
		msg := newScheduleMessage(t, "named")
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				if err := tx.Schedule(
					first, "one", time.Now(), msg,
				); err != nil {
					return err
				}
				if err := tx.Schedule(
					first, "two", time.Now(), msg,
				); err != nil {
					return err
				}
				return tx.Schedule(second, "one", time.Now(), msg)
			}),
		)

		ids, err := backend.ListAggregates("")
		assert.NoError(t, err)
		assert.Len(t, ids, 2)
		firstSchedules, err := store.ListSchedules(first, time.Time{})
		assert.NoError(t, err)
		assert.Len(t, firstSchedules, 2)
		one, err := store.LoadSchedule(first, "one")
		assert.NoError(t, err)
		assert.Equal(t, timebox.ScheduleVersion(0), one.Version)
		two, err := store.LoadSchedule(first, "two")
		assert.NoError(t, err)
		assert.Equal(t, timebox.ScheduleVersion(1), two.Version)
		secondSchedules, err := store.ListSchedules(second, time.Time{})
		assert.NoError(t, err)
		assert.Len(t, secondSchedules, 1)
		assert.Equal(t, timebox.ScheduleVersion(0), secondSchedules[0].Version)
	})

	t.Run("ABA", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		msg := newScheduleMessage(t, "aba")

		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule(testScheduler, "aba", time.Now(), msg)
			}),
		)
		old := loadSchedule(t, store, "aba")
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.CancelSchedule(testScheduler, "aba")
			}),
		)
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule(testScheduler, "aba", time.Now(), msg)
			}),
		)

		err := store.Transact(func(tx *timebox.Transaction) error {
			return tx.ConsumeSchedule(testScheduler, old.Key, old.Version)
		})
		var conflict *timebox.ScheduleVersionConflictError
		assert.ErrorAs(t, err, &conflict)
	})

	t.Run("Replay", func(t *testing.T) {
		cfg := timebox.Config{SnapshotRatio: 0.01}
		backend, store := openBackend(t, p, cfg, nil)
		at := time.Now().UTC().Add(time.Hour)
		msg := newScheduleMessage(t, "replay")
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule(testScheduler, "replay", at, msg)
			}),
		)

		reloaded, err := timebox.NewStore(backend, cfg)
		assert.NoError(t, err)
		schedule := loadSchedule(t, reloaded, "replay")
		assert.Equal(t, at, schedule.At)
		ids, err := backend.ListAggregates("")
		assert.NoError(t, err)
		if !assert.Len(t, ids, 1) {
			t.FailNow()
		}
		assert.NoError(t,
			reloaded.Transact(func(tx *timebox.Transaction) error {
				return tx.ConsumeSchedule(
					testScheduler, schedule.Key, schedule.Version,
				)
			}),
		)
		events, err := backend.LoadEvents(timebox.LoadEventsRequest{
			ID:         ids[0],
			TrimEvents: true,
		})
		assert.NoError(t, err)
		if assert.Len(t, events.Events, 1) {
			assert.Equal(t, int64(1), events.StartSequence)
			assert.Equal(t, int64(1), events.Events[0].Sequence)
		}

		replayed, err := timebox.NewStore(backend, cfg)
		assert.NoError(t, err)
		consumed, err := replayed.LoadSchedule(testScheduler, "replay")
		assert.NoError(t, err)
		assert.Nil(t, consumed)
		events, err = backend.LoadEvents(timebox.LoadEventsRequest{
			ID:         ids[0],
			TrimEvents: true,
		})
		assert.NoError(t, err)
		assert.Empty(t, events.Events)
		snapshot, err := replayed.GetSnapshot[scheduleSnapshot](ids[0])
		assert.NoError(t, err)
		assert.Empty(t, snapshot.State.Schedules)
		assert.Equal(t, timebox.ScheduleVersion(1), snapshot.State.NextVersion)
		assert.NoError(t,
			replayed.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule(testScheduler, "replay", at, msg)
			}),
		)
		rescheduled := loadSchedule(t, replayed, "replay")
		assert.Equal(t, timebox.ScheduleVersion(1), rescheduled.Version)
	})

	t.Run("Prefix", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		msg := newScheduleMessage(t, "prefix")
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				for _, key := range []timebox.ScheduleKey{
					"argyll:flow/one", "argyll:flow/two",
					"argyll:flowish", "foreign",
				} {
					err := tx.Schedule(testScheduler, key, time.Now(), msg)
					if err != nil {
						return err
					}
				}
				return nil
			}),
		)

		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.CancelSchedulePrefix(testScheduler, "argyll:flow/")
			}),
		)
		for _, key := range []timebox.ScheduleKey{
			"argyll:flow/one", "argyll:flow/two",
		} {
			schedule, err := store.LoadSchedule(testScheduler, key)
			assert.NoError(t, err)
			assert.Nil(t, schedule)
		}
		for _, key := range []timebox.ScheduleKey{
			"argyll:flowish", "foreign",
		} {
			schedule, err := store.LoadSchedule(testScheduler, key)
			assert.NoError(t, err)
			assert.NotNil(t, schedule)
		}
	})

	t.Run("Atomic", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		exec := store.Executor(
			func() scheduleState { return scheduleState{} },
			scheduleAppliers.Reduce,
		)
		id := timebox.NewAggregateID("schedule-state", "atomic")
		msg := newScheduleMessage(t, "atomic")
		err := store.Transact(func(tx *timebox.Transaction) error {
			if _, err := tx.Exec(exec, id, changeScheduleState(1)); err != nil {
				return err
			}
			return tx.Schedule(testScheduler, "atomic", time.Now(), msg)
		})
		assert.NoError(t, err)

		schedule := loadSchedule(t, store, "atomic")
		err = store.Transact(func(tx *timebox.Transaction) error {
			if err := tx.ConsumeSchedule(testScheduler,
				schedule.Key, schedule.Version); err != nil {
				return err
			}
			_, err := tx.Exec(exec, id, changeScheduleState(1))
			return err
		})
		assert.NoError(t, err)

		consumed, err := store.LoadSchedule(testScheduler, "atomic")
		assert.NoError(t, err)
		assert.Nil(t, consumed)
		st, err := exec.Get(id)
		assert.NoError(t, err)
		assert.Equal(t, 2, st.Value)
	})

	t.Run("List", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		msg := newScheduleMessage(t, "order")
		base := time.Now().UTC()
		for _, item := range []scheduleItem{
			{key: "later", at: base.Add(2 * time.Hour)},
			{key: "first", at: base.Add(time.Hour)},
			{key: "second", at: base.Add(time.Hour)},
		} {
			assert.NoError(t,
				store.Transact(func(tx *timebox.Transaction) error {
					return tx.Schedule(testScheduler, item.key, item.at, msg)
				}),
			)
		}

		schedules, err := store.ListSchedules(testScheduler, time.Time{})
		assert.NoError(t, err)
		keys := make([]timebox.ScheduleKey, len(schedules))
		for i, schedule := range schedules {
			keys[i] = schedule.Key
		}
		assert.ElementsMatch(t,
			[]timebox.ScheduleKey{"first", "second", "later"}, keys,
		)

		due, err := store.ListSchedules(testScheduler, base.Add(time.Hour))
		assert.NoError(t, err)
		keys = make([]timebox.ScheduleKey, len(due))
		for i, schedule := range due {
			keys[i] = schedule.Key
		}
		assert.ElementsMatch(t,
			[]timebox.ScheduleKey{"first", "second"}, keys,
		)

		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule(
					testScheduler, "later", base.Add(30*time.Minute), msg,
				)
			}),
		)
		due, err = store.ListSchedules(testScheduler, base.Add(time.Hour))
		assert.NoError(t, err)
		keys = make([]timebox.ScheduleKey, len(due))
		for i, schedule := range due {
			keys[i] = schedule.Key
		}
		assert.ElementsMatch(t,
			[]timebox.ScheduleKey{"first", "second", "later"}, keys,
		)
	})
}

func newScheduleMessage(t *testing.T, key string) *timebox.Message {
	t.Helper()
	data, err := json.Marshal(scheduleData{Value: 1})
	if !assert.NoError(t, err) {
		return nil
	}
	return &timebox.Message{
		AggregateID: timebox.NewAggregateID("schedule-target", timebox.ID(key)),
		Type:        "schedule.test",
		Data:        data,
	}
}

func loadSchedule(
	t *testing.T, store *timebox.Store, key timebox.ScheduleKey,
) *timebox.Schedule {
	t.Helper()
	schedule, err := store.LoadSchedule(testScheduler, key)
	if !assert.NoError(t, err) || !assert.NotNil(t, schedule) {
		return &timebox.Schedule{Message: &timebox.Message{}}
	}
	return schedule
}

func changeScheduleState(value int) timebox.Command[scheduleState] {
	return func(_ scheduleState, ag *timebox.Aggregator[scheduleState]) error {
		return ag.Raise(scheduleChanged, value)
	}
}

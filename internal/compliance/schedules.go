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
)

const scheduleChanged timebox.EventType = "schedule.changed"

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
		event := newScheduleEvent(t, "one")

		err := store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("one", at, event)
		})
		assert.NoError(t, err)

		first := loadSchedule(t, store, "one")
		assert.Equal(t, timebox.ScheduleVersion(0), first.Version)

		err = store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("one", at.Add(time.Hour), event)
		})
		assert.NoError(t, err)
		second := loadSchedule(t, store, "one")
		assert.Greater(t, second.Version, first.Version)

		err = store.Transact(func(tx *timebox.Transaction) error {
			return tx.ConsumeSchedule(second.Key, second.Version)
		})
		assert.NoError(t, err)
		consumed, err := store.LoadSchedule("one")
		assert.NoError(t, err)
		assert.Nil(t, consumed)
	})

	t.Run("ABA", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		event := newScheduleEvent(t, "aba")

		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule("aba", time.Now(), event)
			}),
		)
		old := loadSchedule(t, store, "aba")
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.CancelSchedule("aba")
			}),
		)
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule("aba", time.Now(), event)
			}),
		)

		err := store.Transact(func(tx *timebox.Transaction) error {
			return tx.ConsumeSchedule(old.Key, old.Version)
		})
		var conflict *timebox.ScheduleVersionConflictError
		assert.ErrorAs(t, err, &conflict)
	})

	t.Run("Prefix", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		event := newScheduleEvent(t, "prefix")
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				for _, key := range []timebox.ScheduleKey{
					"argyll:flow/one", "argyll:flow/two",
					"argyll:flowish", "foreign",
				} {
					if err := tx.Schedule(key, time.Now(), event); err != nil {
						return err
					}
				}
				return nil
			}),
		)

		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.CancelSchedulePrefix("argyll:flow/")
			}),
		)
		for _, key := range []timebox.ScheduleKey{
			"argyll:flow/one", "argyll:flow/two",
		} {
			schedule, err := store.LoadSchedule(key)
			assert.NoError(t, err)
			assert.Nil(t, schedule)
		}
		for _, key := range []timebox.ScheduleKey{
			"argyll:flowish", "foreign",
		} {
			schedule, err := store.LoadSchedule(key)
			assert.NoError(t, err)
			assert.NotNil(t, schedule)
		}
	})

	t.Run("Atomic", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		exec := store.Executor(
			func() scheduleState { return scheduleState{} },
			scheduleAppliers,
		)
		id := timebox.NewAggregateID("schedule-state", "atomic")
		event := newScheduleEvent(t, "atomic")
		err := store.Transact(func(tx *timebox.Transaction) error {
			if _, err := tx.Exec(exec, id, changeScheduleState(1)); err != nil {
				return err
			}
			return tx.Schedule("atomic", time.Now(), event)
		})
		assert.NoError(t, err)

		schedule := loadSchedule(t, store, "atomic")
		err = store.Transact(func(tx *timebox.Transaction) error {
			if err := tx.ConsumeSchedule(
				schedule.Key, schedule.Version,
			); err != nil {
				return err
			}
			_, err := tx.Exec(exec, id, changeScheduleState(1))
			return err
		})
		assert.NoError(t, err)

		consumed, err := store.LoadSchedule("atomic")
		assert.NoError(t, err)
		assert.Nil(t, consumed)
		st, err := exec.Get(id)
		assert.NoError(t, err)
		assert.Equal(t, 2, st.Value)
	})

	t.Run("List", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		event := newScheduleEvent(t, "order")
		base := time.Now().UTC()
		for _, item := range []scheduleItem{
			{key: "later", at: base.Add(2 * time.Hour)},
			{key: "first", at: base.Add(time.Hour)},
			{key: "second", at: base.Add(time.Hour)},
		} {
			assert.NoError(t,
				store.Transact(func(tx *timebox.Transaction) error {
					return tx.Schedule(item.key, item.at, event)
				}),
			)
		}

		schedules, err := store.ListSchedules(time.Time{})
		assert.NoError(t, err)
		keys := make([]timebox.ScheduleKey, len(schedules))
		for i, schedule := range schedules {
			keys[i] = schedule.Key
		}
		assert.ElementsMatch(t,
			[]timebox.ScheduleKey{"first", "second", "later"}, keys,
		)

		due, err := store.ListSchedules(base.Add(time.Hour))
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
					"later", base.Add(30*time.Minute), event,
				)
			}),
		)
		due, err = store.ListSchedules(base.Add(time.Hour))
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

func newScheduleEvent(t *testing.T, key string) *timebox.Event {
	t.Helper()
	data, err := json.Marshal(scheduleData{Value: 1})
	if !assert.NoError(t, err) {
		return nil
	}
	return &timebox.Event{
		AggregateID: timebox.NewAggregateID("schedule-target", timebox.ID(key)),
		Type:        "schedule.test",
		Data:        data,
	}
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

func changeScheduleState(value int) timebox.Command[scheduleState] {
	return func(_ scheduleState, ag *timebox.Aggregator[scheduleState]) error {
		return ag.Raise(scheduleChanged, value)
	}
}

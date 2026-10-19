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

	scheduleChange struct {
		name   string
		change func(*timebox.Schedule)
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
		backend, store := openBackend(t, p, timebox.Config{}, nil)
		at := time.Now().UTC().Add(time.Hour)
		msg := newScheduleMessage(t, "one")

		err := store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("one", at, msg)
		})
		assert.NoError(t, err)

		first := loadSchedule(t, store, "one")
		store.TakeScheduleChanges()
		err = store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("one", at.Local(), newScheduleMessage(t, "one"))
		})
		assert.NoError(t, err)
		assert.Equal(t, first, loadSchedule(t, store, "one"))
		assert.Empty(t, store.TakeScheduleChanges())

		err = store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("one", at.Add(time.Hour), msg)
		})
		assert.NoError(t, err)
		second := loadSchedule(t, store, "one")
		assert.Equal(t, at.Add(time.Hour), second.At)

		var consumed *timebox.Schedule
		err = store.Transact(func(tx *timebox.Transaction) error {
			var err error
			consumed, err = tx.ConsumeSchedule("one", second.At)
			return err
		})
		assert.NoError(t, err)
		assert.Equal(t, second, consumed)
		consumed, err = store.LoadSchedule("one")
		assert.NoError(t, err)
		assert.Nil(t, consumed)
		ids, err := backend.ListAggregates("")
		assert.NoError(t, err)
		assert.Empty(t, ids)
	})

	t.Run("Replace", func(t *testing.T) {
		for _, tc := range []scheduleChange{
			{name: "Deadline", change: func(s *timebox.Schedule) {
				s.At = s.At.Add(time.Hour)
			}},
			{name: "Type", change: func(s *timebox.Schedule) {
				s.Message.Type = "other"
			}},
			{name: "TargetType", change: func(s *timebox.Schedule) {
				s.Message.AggregateID.Type = "other"
			}},
			{name: "TargetKey", change: func(s *timebox.Schedule) {
				s.Message.AggregateID.Key = "other"
			}},
			{name: "Data", change: func(s *timebox.Schedule) {
				s.Message.Data = json.RawMessage(`{"Value":2}`)
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				store := openStore(t, p, timebox.Config{})
				at := time.Now().UTC().Add(time.Hour)
				msg := newScheduleMessage(t, "replace")
				err := store.Transact(func(tx *timebox.Transaction) error {
					return tx.Schedule("replace", at, msg)
				})
				assert.NoError(t, err)
				first := loadSchedule(t, store, "replace")
				tc.change(first)
				err = store.Transact(func(tx *timebox.Transaction) error {
					return tx.Schedule(first.Key, first.At, first.Message)
				})
				assert.NoError(t, err)
				next := loadSchedule(t, store, "replace")
				assert.Equal(t, first.At, next.At)
				assert.Equal(t, first.Message, next.Message)
			})
		}
	})

	t.Run("Refresh", func(t *testing.T) {
		backend, first := openBackend(t, p, timebox.Config{}, nil)
		second, err := timebox.NewStore(backend)
		assert.NoError(t, err)
		at := time.Now().UTC().Add(time.Hour)
		msg := newScheduleMessage(t, "refresh")
		assert.NoError(t, first.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("refresh", at, msg)
		}))
		assert.NoError(t, second.Transact(func(tx *timebox.Transaction) error {
			_, err := tx.ConsumeSchedule("refresh", at)
			return err
		}))
		assert.NoError(t, second.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("refresh", at.Add(time.Hour), msg)
		}))
		assert.NoError(t, first.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("refresh", at, msg)
		}))
		assert.Equal(t, at, loadSchedule(t, first, "refresh").At)
	})

	t.Run("ConsumeReschedule", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		msg := newScheduleMessage(t, "repeat")
		at := time.Now().UTC().Add(time.Hour)
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule("repeat", at, msg)
			}),
		)
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				if _, err := tx.ConsumeSchedule("repeat", at); err != nil {
					return err
				}
				return tx.Schedule("repeat", at, msg)
			}),
		)
		next := loadSchedule(t, store, "repeat")
		assert.Equal(t, at, next.At)

		due, err := store.ListSchedules(at)
		assert.NoError(t, err)
		assert.Len(t, due, 1)
	})

	t.Run("NotDue", func(t *testing.T) {
		store := openStore(t, p, timebox.Config{})
		at := time.Now().UTC().Add(time.Hour)
		msg := newScheduleMessage(t, "later")
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule("later", at, msg)
			}),
		)
		var consumed *timebox.Schedule
		assert.NoError(t,
			store.Transact(func(tx *timebox.Transaction) error {
				var err error
				consumed, err = tx.ConsumeSchedule("later", time.Now())
				return err
			}),
		)
		assert.Nil(t, consumed)
		assert.Equal(t, at, loadSchedule(t, store, "later").At)
	})

	t.Run("Stale", func(t *testing.T) {
		backend, first := openBackend(t, p, timebox.Config{}, nil)
		second, err := timebox.NewStore(backend)
		assert.NoError(t, err)
		msg := newScheduleMessage(t, "stale")
		at := time.Now().UTC()
		assert.NoError(t, first.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("stale", at, msg)
		}))
		assert.NoError(t, second.Transact(func(tx *timebox.Transaction) error {
			_, err := tx.ConsumeSchedule("stale", at)
			return err
		}))

		var consumed *timebox.Schedule
		assert.NoError(t, first.Transact(func(tx *timebox.Transaction) error {
			var err error
			consumed, err = tx.ConsumeSchedule("stale", at)
			return err
		}))
		assert.Nil(t, consumed)
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
			return tx.Schedule("atomic", time.Now(), msg)
		})
		assert.NoError(t, err)

		schedule := loadSchedule(t, store, "atomic")
		err = store.Transact(func(tx *timebox.Transaction) error {
			_, err := tx.ConsumeSchedule(schedule.Key, schedule.At)
			if err != nil {
				return err
			}
			_, err = tx.Exec(exec, id, changeScheduleState(1))
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
		msg := newScheduleMessage(t, "order")
		base := time.Now().UTC()
		for _, item := range []scheduleItem{
			{key: "later", at: base.Add(2 * time.Hour)},
			{key: "first", at: base.Add(time.Hour)},
			{key: "second", at: base.Add(time.Hour)},
		} {
			assert.NoError(t,
				store.Transact(func(tx *timebox.Transaction) error {
					return tx.Schedule(item.key, item.at, msg)
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
				return tx.Schedule("later", base.Add(30*time.Minute), msg)
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
	schedule, err := store.LoadSchedule(key)
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

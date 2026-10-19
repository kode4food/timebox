package timebox_test

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
)

type (
	scheduleBackend struct {
		timebox.Backend
		requests []timebox.AppendRequest
	}

	counter struct {
		Value int
	}

	payload struct {
		Name string
	}

	listItem struct {
		key string
		at  time.Time
	}

	validationCase struct {
		name    string
		key     timebox.ScheduleKey
		message *timebox.Message
		err     error
	}
)

const counterChanged timebox.EventType = "counter.changed"

var (
	errStop = errors.New("stop")
)

// Append records the latest request batch before appending it
func (s *scheduleBackend) Append(reqs ...timebox.AppendRequest) error {
	s.requests = reqs
	return s.Backend.Append(reqs...)
}

func TestSchedule(t *testing.T) {
	store, exec := newStore(t)
	target := timebox.NewAggregateID("counter", "one")
	at := time.Now().UTC().Add(time.Hour)
	msg := newTargetMessage(t, target, "first")

	err := store.Transact(func(tx *timebox.Transaction) error {
		if _, err := tx.Exec(exec, target, changeCounter(1)); err != nil {
			return err
		}
		return tx.Schedule("counter/one/expire", at, msg)
	})
	assert.NoError(t, err)

	schedule := loadSchedule(t, store, "counter/one/expire")
	assert.Equal(t, at, schedule.At)
	assert.Equal(t, target, schedule.Message.AggregateID)

	got, err := schedule.Message.GetValue[payload]()
	assert.NoError(t, err)
	assert.Equal(t, payload{Name: "first"}, got)
}

func TestScheduleNoop(t *testing.T) {
	backend := &scheduleBackend{Backend: memory.Open()}
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })
	store, err := timebox.NewStore(backend)
	assert.NoError(t, err)
	at := time.Now().Add(time.Hour)
	msg := newMessage(t, "same")
	assert.NoError(t, store.Transact(func(tx *timebox.Transaction) error {
		if err := tx.Schedule("same", at, msg); err != nil {
			return err
		}
		return tx.Schedule("same", at, msg)
	}))
	if assert.Len(t, backend.requests, 1) {
		assert.Len(t, backend.requests[0].Events, 1)
	}
	first := loadSchedule(t, store, "same")

	assert.NoError(t, store.Transact(func(tx *timebox.Transaction) error {
		return tx.Schedule("same", at.UTC(), newMessage(t, "same"))
	}))
	if assert.Len(t, backend.requests, 1) {
		assert.Empty(t, backend.requests[0].Events)
		assert.Nil(t, backend.requests[0].Status)
	}
	assert.Equal(t, first, loadSchedule(t, store, "same"))
}

func TestScheduleCompose(t *testing.T) {
	store, _ := newStore(t)
	first := newMessage(t, "first")
	second := newMessage(t, "second")

	assert.NoError(t,
		store.Transact(func(tx *timebox.Transaction) error {
			if err := tx.Schedule("compose", time.Now(), first); err != nil {
				return err
			}
			return tx.Schedule("compose", time.Now().Add(time.Hour), second)
		}),
	)
	schedule := loadSchedule(t, store, "compose")
	got, err := schedule.Message.GetValue[payload]()
	assert.NoError(t, err)
	assert.Equal(t, payload{Name: "second"}, got)

	later := time.Now().UTC().Add(2 * time.Hour)
	assert.NoError(t,
		store.Transact(func(tx *timebox.Transaction) error {
			_, err := tx.ConsumeSchedule(schedule.Key, schedule.At)
			if err != nil {
				return err
			}
			return tx.Schedule("compose", later, first)
		}),
	)
	assert.Equal(t, later, loadSchedule(t, store, "compose").At)

	var consumed *timebox.Schedule
	err = store.Transact(func(tx *timebox.Transaction) error {
		if err := tx.Schedule("compose", time.Now(), second); err != nil {
			return err
		}
		consumed, err = tx.ConsumeSchedule("compose", time.Now())
		return err
	})
	assert.NoError(t, err)
	if assert.NotNil(t, consumed) {
		assert.Equal(t, second, consumed.Message)
	}
	remaining, err := store.LoadSchedule("compose")
	assert.NoError(t, err)
	assert.Nil(t, remaining)
}

func TestScheduleRefresh(t *testing.T) {
	backend := memory.Open()
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })
	first, err := backend.NewStore()
	assert.NoError(t, err)
	second, err := backend.NewStore()
	assert.NoError(t, err)

	assert.NoError(t,
		first.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule(
				"remote", time.Now().Add(time.Hour),
				newMessage(t, "first"),
			)
		}),
	)
	original := loadSchedule(t, second, "remote")

	assert.NoError(t,
		first.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule(
				"remote", time.Now().Add(2*time.Hour),
				newMessage(t, "second"),
			)
		}),
	)
	updated := loadSchedule(t, second, "remote")
	assert.True(t, updated.At.After(original.At))
}

func TestScheduleConsume(t *testing.T) {
	store, exec := newStore(t)
	target := timebox.NewAggregateID("counter", "consume")
	msg := newMessage(t, "consume")

	err := store.Transact(func(tx *timebox.Transaction) error {
		return tx.Schedule("consume", time.Now(), msg)
	})
	assert.NoError(t, err)
	schedule := loadSchedule(t, store, "consume")

	err = store.Transact(func(tx *timebox.Transaction) error {
		_, err := tx.ConsumeSchedule(schedule.Key, schedule.At)
		if err != nil {
			return err
		}
		_, err = tx.Exec(exec, target, changeCounter(2))
		return err
	})
	assert.NoError(t, err)

	consumed, err := store.LoadSchedule("consume")
	assert.NoError(t, err)
	assert.Nil(t, consumed)
	st, err := exec.Get(target)
	assert.NoError(t, err)
	assert.Equal(t, 2, st.Value)
}

func TestScheduleConsumeNoSnapshot(t *testing.T) {
	store, _ := newStore(t)
	msg := newMessage(t, "fresh")
	assert.NoError(t, store.Transact(func(tx *timebox.Transaction) error {
		return tx.Schedule("fresh", time.Now(), msg)
	}))
	schedule := loadSchedule(t, store, "fresh")

	errAbort := errors.New("abort")
	err := store.Transact(func(tx *timebox.Transaction) error {
		_, err := tx.ConsumeSchedule(schedule.Key, schedule.At)
		if err != nil {
			return err
		}
		return errAbort
	})
	assert.ErrorIs(t, err, errAbort)

	snap, err := store.GetSnapshot[json.RawMessage](
		timebox.NewAggregateID("_tb.sched_", "fresh"),
	)
	assert.NoError(t, err)
	assert.Zero(t, snap.SnapshotSize)
}

func TestScheduleRollback(t *testing.T) {
	store, _ := newStore(t)
	msg := newMessage(t, "rollback")

	err := store.Transact(func(tx *timebox.Transaction) error {
		if err := tx.Schedule("rollback", time.Now(), msg); err != nil {
			return err
		}
		return errStop
	})
	assert.ErrorIs(t, err, errStop)

	schedule, err := store.LoadSchedule("rollback")
	assert.NoError(t, err)
	assert.Nil(t, schedule)
}

func TestScheduleList(t *testing.T) {
	store, _ := newStore(t)
	base := time.Now().UTC()
	for _, item := range []listItem{
		{key: "third", at: base.Add(3 * time.Hour)},
		{key: "second", at: base.Add(2 * time.Hour)},
		{key: "first", at: base.Add(time.Hour)},
	} {
		err := store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule(
				timebox.ScheduleKey(item.key), item.at,
				newMessage(t, item.key),
			)
		})
		assert.NoError(t, err)
	}

	schedules, err := store.ListSchedules(time.Time{})
	assert.NoError(t, err)
	keys := make([]timebox.ScheduleKey, len(schedules))
	for i, schedule := range schedules {
		keys[i] = schedule.Key
	}
	assert.ElementsMatch(t,
		[]timebox.ScheduleKey{"first", "second", "third"}, keys,
	)

	due, err := store.ListSchedules(base.Add(2 * time.Hour))
	assert.NoError(t, err)
	keys = make([]timebox.ScheduleKey, len(due))
	for i, schedule := range due {
		keys[i] = schedule.Key
	}
	assert.Equal(t,
		[]timebox.ScheduleKey{"first", "second"}, keys,
	)
}

func TestScheduleRecovery(t *testing.T) {
	backend := memory.Open()
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })
	first, err := backend.NewStore()
	assert.NoError(t, err)
	msg := newMessage(t, "restart")
	at := time.Now().UTC().Add(time.Hour)
	assert.NoError(t,
		first.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("restart", at, msg)
		}),
	)

	second, err := backend.NewStore()
	assert.NoError(t, err)
	loaded := loadSchedule(t, second, "restart")

	third, err := backend.NewStore()
	assert.NoError(t, err)
	recovered := loadSchedule(t, third, "restart")
	assert.Equal(t, loaded, recovered)
	assert.Equal(t, msg, recovered.Message)
}

func TestScheduleJSON(t *testing.T) {
	data := []byte(`{"Message":{"type":"schedule.test",` +
		`"aggregate_id":["target","one"],"data":{"name":"one"}},` +
		`"Key":"one"}`)
	var schedule timebox.Schedule
	assert.NoError(t, json.Unmarshal(data, &schedule))
	assert.Equal(t, timebox.ScheduleKey("one"), schedule.Key)
	assert.Equal(t, timebox.EventType("schedule.test"),
		schedule.Message.Type)

	encoded, err := json.Marshal(schedule)
	assert.NoError(t, err)
	var fields map[string]json.RawMessage
	assert.NoError(t, json.Unmarshal(encoded, &fields))
	assert.Contains(t, fields, "Message")
	assert.NotContains(t, fields, "Event")
}

func TestScheduleInvalid(t *testing.T) {
	store, _ := newStore(t)
	valid := newMessage(t, "valid")
	cases := []validationCase{
		{
			name: "Key", message: valid,
			err: timebox.ErrScheduleKeyRequired,
		},
		{
			name: "Message", key: "invalid",
			err: timebox.ErrScheduleMessageRequired,
		},
		{
			name: "Type", key: "invalid",
			message: &timebox.Message{
				AggregateID: timebox.NewAggregateID("target", "type"),
			},
			err: timebox.ErrScheduleMessageTypeRequired,
		},
		{
			name: "ID", key: "invalid",
			message: &timebox.Message{Type: "schedule.test"},
			err:     timebox.ErrInvalidAggregateID,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := store.Transact(func(tx *timebox.Transaction) error {
				return tx.Schedule(
					tc.key, time.Now(), tc.message,
				)
			})
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

func TestScheduleConcurrent(t *testing.T) {
	store, exec := newStore(t)
	target := timebox.NewAggregateID("counter", "concurrent")
	msg := newMessage(t, "concurrent")
	assert.NoError(t,
		store.Transact(func(tx *timebox.Transaction) error {
			return tx.Schedule("concurrent", time.Now(), msg)
		}),
	)
	schedule := loadSchedule(t, store, "concurrent")

	start := make(chan struct{})
	errs := make(chan error, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			first := true
			errs <- store.Transact(func(tx *timebox.Transaction) error {
				if first {
					first = false
					ready.Done()
					<-start
				}
				consumed, err := tx.ConsumeSchedule(schedule.Key, schedule.At)
				if err != nil || consumed == nil {
					return err
				}
				_, err = tx.Exec(exec, target, changeCounter(1))
				return err
			})
		}()
	}
	ready.Wait()
	close(start)

	for range 2 {
		assert.NoError(t, <-errs)
	}
	remaining, err := store.LoadSchedule("concurrent")
	assert.NoError(t, err)
	assert.Nil(t, remaining)
	st, err := exec.Get(target)
	assert.NoError(t, err)
	assert.Equal(t, 1, st.Value)
}

func newStore(t *testing.T) (*timebox.Store, *timebox.Executor[counter]) {
	t.Helper()
	backend := memory.Open()
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })
	store, err := backend.NewStore()
	assert.NoError(t, err)
	exec := store.Executor(
		func() counter { return counter{} },
		(timebox.Appliers[counter]{
			counterChanged: timebox.MakeApplier(
				func(st counter, _ *timebox.Event, value int) counter {
					st.Value += value
					return st
				},
			),
		}).Reduce,
	)
	return store, exec
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

func newMessage(t *testing.T, name string) *timebox.Message {
	t.Helper()
	id := timebox.NewAggregateID("schedule-target", timebox.ID(name))
	return newTargetMessage(t, id, name)
}

func newTargetMessage(
	t *testing.T, id timebox.AggregateID, name string,
) *timebox.Message {
	t.Helper()
	data, err := json.Marshal(payload{Name: name})
	if !assert.NoError(t, err) {
		return nil
	}
	return &timebox.Message{
		AggregateID: id,
		Type:        "test.schedule",
		Data:        data,
	}
}

func changeCounter(value int) timebox.Command[counter] {
	return func(_ counter, ag *timebox.Aggregator[counter]) error {
		return ag.Raise(counterChanged, value)
	}
}

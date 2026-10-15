package timebox

import (
	"container/heap"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

type (
	// Schedule is one durable deferred message and its delivery metadata
	Schedule struct {
		Message *Message
		At      time.Time
		Key     ScheduleKey
		Version ScheduleVersion
	}

	// ScheduleVersionConflictError indicates a stale schedule incarnation
	ScheduleVersionConflictError struct {
		Key             ScheduleKey
		ExpectedVersion ScheduleVersion
	}

	// ScheduleKey identifies one replaceable deferred message
	ScheduleKey string

	// ScheduleVersion identifies one incarnation within a scheduler
	ScheduleVersion int64

	// Schedules is a heap of active immutable schedules
	Schedules []*Schedule

	scheduleState struct {
		Schedules   Schedules       `json:"schedules"`
		NextVersion ScheduleVersion `json:"next_version"`
	}

	scheduleHeap struct {
		items   *Schedules
		indices map[ScheduleKey]int
	}
)

const (
	scheduleAggregateType ID        = "_tb.sched_"
	scheduleChanged       EventType = "_tb.sched.changed"
	scheduleCanceled      EventType = "_tb.sched.canceled"
	scheduleConsumed      EventType = "_tb.sched.consumed"
)

var (
	// ErrSchedulerIDRequired indicates a scheduler ID was empty
	ErrSchedulerIDRequired = errors.New("scheduler ID is required")

	// ErrScheduleKeyRequired indicates a schedule key was empty
	ErrScheduleKeyRequired = errors.New("schedule key is required")

	// ErrScheduleMessageRequired indicates a schedule has no deferred message
	ErrScheduleMessageRequired = errors.New("schedule message is required")

	// ErrScheduleMessageTypeRequired indicates an empty deferred message type
	ErrScheduleMessageTypeRequired = errors.New(
		"schedule message type is required",
	)
)

// Error describes a stale schedule incarnation
func (s *ScheduleVersionConflictError) Error() string {
	return fmt.Sprintf("schedule version conflict for %q: expected %d",
		s.Key, s.ExpectedVersion)
}

// Schedule creates or replaces a durable deferred message
func (t *Transaction) Schedule(
	scheduler ID, key ScheduleKey, at time.Time, message *Message,
) error {
	if err := validateSchedule(scheduler, key, message); err != nil {
		return err
	}
	_, err := t.execInternal(t.store.schedule, makeScheduleID(scheduler),
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			return ag.Raise(scheduleChanged, &Schedule{
				Message: message,
				At:      at.UTC(),
				Key:     key,
				Version: st.NextVersion,
			})
		},
	)
	return err
}

// CancelSchedule removes the current schedule for a key
func (t *Transaction) CancelSchedule(scheduler ID, key ScheduleKey) error {
	if err := validateScheduleKey(scheduler, key); err != nil {
		return err
	}
	_, err := t.execInternal(t.store.schedule, makeScheduleID(scheduler),
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			schedule := findSchedule(st.Schedules, key)
			if schedule == nil {
				return nil
			}
			return ag.Raise(scheduleCanceled, []ScheduleKey{schedule.Key})
		},
	)
	return err
}

// CancelSchedulePrefix removes schedules whose keys share a prefix
func (t *Transaction) CancelSchedulePrefix(
	scheduler ID, prefix ScheduleKey,
) error {
	if err := validateScheduleKey(scheduler, prefix); err != nil {
		return err
	}
	_, err := t.execInternal(t.store.schedule, makeScheduleID(scheduler),
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			keys := make([]ScheduleKey, 0)
			for _, schedule := range st.Schedules {
				if strings.HasPrefix(
					string(schedule.Key), string(prefix),
				) {
					keys = append(keys, schedule.Key)
				}
			}
			slices.Sort(keys)
			if len(keys) == 0 {
				return nil
			}
			return ag.Raise(scheduleCanceled, keys)
		},
	)
	return err
}

// ConsumeSchedule conditionally removes one observed schedule incarnation
func (t *Transaction) ConsumeSchedule(
	scheduler ID, key ScheduleKey, version ScheduleVersion,
) error {
	if err := validateScheduleKey(scheduler, key); err != nil {
		return err
	}
	_, err := t.execInternal(t.store.schedule, makeScheduleID(scheduler),
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			schedule := findSchedule(st.Schedules, key)
			if schedule == nil || schedule.Version != version {
				return &ScheduleVersionConflictError{
					Key:             key,
					ExpectedVersion: version,
				}
			}
			return ag.Raise(scheduleConsumed, key)
		},
	)
	return err
}

// ScheduleChanges reports coalesced local schedule commit notifications
func (s *Store) ScheduleChanges() <-chan struct{} {
	return s.changes
}

// LoadSchedule loads one active schedule from a named scheduler
func (s *Store) LoadSchedule(scheduler ID, key ScheduleKey) (*Schedule, error) {
	if err := validateScheduleKey(scheduler, key); err != nil {
		return nil, err
	}
	st, err := s.loadScheduleState(scheduler)
	if err != nil {
		return nil, err
	}
	schedule := findSchedule(st.Schedules, key)
	if schedule == nil {
		return nil, nil
	}
	return schedule, nil
}

// ListSchedules lists schedules through a time, or all when through is zero
func (s *Store) ListSchedules(
	scheduler ID, through time.Time,
) ([]*Schedule, error) {
	if scheduler == "" {
		return nil, ErrSchedulerIDRequired
	}
	st, err := s.loadScheduleState(scheduler)
	if err != nil {
		return nil, err
	}
	if through.IsZero() {
		return slices.Clone(st.Schedules), nil
	}
	return schedulesThrough(st.Schedules, through), nil
}

func (s *Store) notifyScheduleChange(_ scheduleState, evs []*Event) {
	if len(evs) == 0 {
		return
	}
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

func (s *Store) loadScheduleState(scheduler ID) (scheduleState, error) {
	id := makeScheduleID(scheduler)
	s.schedule.invalidate(id)
	var res scheduleState
	err := s.Transact(func(t *Transaction) error {
		var err error
		res, err = t.execInternal(
			s.schedule, id,
			func(scheduleState, *Aggregator[scheduleState]) error {
				return nil
			},
		)
		return err
	})
	return res, err
}

func (h *scheduleHeap) Replace(schedule *Schedule) {
	if i, ok := h.indices[schedule.Key]; ok {
		(*h.items)[i] = schedule
		heap.Fix(h, i)
		return
	}
	heap.Push(h, schedule)
}

func (h *scheduleHeap) Remove(key ScheduleKey) {
	if i, ok := h.indices[key]; ok {
		heap.Remove(h, i)
	}
}

func (h *scheduleHeap) Len() int {
	return len(*h.items)
}

func (h *scheduleHeap) Less(i, j int) bool {
	l := (*h.items)[i]
	r := (*h.items)[j]
	if l.At.Equal(r.At) {
		return l.Key < r.Key
	}
	return l.At.Before(r.At)
}

func (h *scheduleHeap) Swap(i, j int) {
	(*h.items)[i], (*h.items)[j] = (*h.items)[j], (*h.items)[i]
	h.indices[(*h.items)[i].Key] = i
	h.indices[(*h.items)[j].Key] = j
}

func (h *scheduleHeap) Push(value any) {
	schedule := value.(*Schedule)
	h.indices[schedule.Key] = len(*h.items)
	*h.items = append(*h.items, schedule)
}

func (h *scheduleHeap) Pop() any {
	last := len(*h.items) - 1
	schedule := (*h.items)[last]
	(*h.items)[last] = nil
	*h.items = (*h.items)[:last]
	delete(h.indices, schedule.Key)
	return schedule
}

func newScheduleState() scheduleState {
	return scheduleState{Schedules: Schedules{}}
}

func reduceSchedules(st scheduleState, evs []*Event) scheduleState {
	if len(evs) == 0 {
		return st
	}
	st.Schedules = slices.Clone(st.Schedules)
	h := newScheduleHeap(&st.Schedules)
	for _, ev := range evs {
		switch ev.Type {
		case scheduleChanged:
			schedule, err := ev.GetValue[*Schedule]()
			if err == nil {
				h.Replace(schedule)
				st.NextVersion = schedule.Version + 1
			}
		case scheduleCanceled:
			keys, err := ev.GetValue[[]ScheduleKey]()
			if err == nil {
				for _, key := range keys {
					h.Remove(key)
				}
			}
		case scheduleConsumed:
			key, err := ev.GetValue[ScheduleKey]()
			if err == nil {
				h.Remove(key)
			}
		}
	}
	return st
}

func newScheduleHeap(items *Schedules) *scheduleHeap {
	indices := make(map[ScheduleKey]int, len(*items))
	for i, schedule := range *items {
		indices[schedule.Key] = i
	}
	return &scheduleHeap{items: items, indices: indices}
}

func findSchedule(schedules Schedules, key ScheduleKey) *Schedule {
	for _, schedule := range schedules {
		if schedule.Key == key {
			return schedule
		}
	}
	return nil
}

func schedulesThrough(schedules Schedules, through time.Time) []*Schedule {
	res := make([]*Schedule, 0)
	visit := []int{0}
	for len(visit) > 0 {
		last := len(visit) - 1
		i := visit[last]
		visit = visit[:last]
		if i >= len(schedules) || schedules[i].At.After(through) {
			continue
		}
		res = append(res, schedules[i])
		visit = append(visit, 2*i+1, 2*i+2)
	}
	return res
}

func makeScheduleID(scheduler ID) AggregateID {
	return NewAggregateID(scheduleAggregateType, scheduler)
}

func validateSchedule(
	scheduler ID, key ScheduleKey, message *Message,
) error {
	if err := validateScheduleKey(scheduler, key); err != nil {
		return err
	}
	if message == nil {
		return ErrScheduleMessageRequired
	}
	if message.Type == "" {
		return ErrScheduleMessageTypeRequired
	}
	if message.AggregateID.Type == "" || message.AggregateID.Key == "" {
		return fmt.Errorf("%w: schedule message", ErrInvalidAggregateID)
	}
	return nil
}

func validateScheduleKey(scheduler ID, key ScheduleKey) error {
	if scheduler == "" {
		return ErrSchedulerIDRequired
	}
	if key == "" {
		return ErrScheduleKeyRequired
	}
	return nil
}

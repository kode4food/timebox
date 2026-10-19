package timebox

import (
	"errors"
	"fmt"
	"time"
)

type (
	// Schedule is one durable deferred message and its delivery metadata
	Schedule struct {
		Message *Message
		At      time.Time
		Key     ScheduleKey
	}

	// ScheduleKey identifies one replaceable deferred message
	ScheduleKey string

	scheduleState struct {
		Active *Schedule `json:"active,omitempty"`
	}
)

const (
	scheduleAggregateType ID        = "_tb.sched_"
	scheduleChanged       EventType = "_tb.sched.changed"
	scheduleConsumed      EventType = "_tb.sched.consumed"
	scheduleActiveStatus            = "_tb.sched.active"
)

var (
	// ErrScheduleKeyRequired indicates a schedule key was empty
	ErrScheduleKeyRequired = errors.New("schedule key is required")

	// ErrScheduleMessageRequired indicates a schedule has no deferred message
	ErrScheduleMessageRequired = errors.New("schedule message is required")

	// ErrScheduleMessageTypeRequired indicates an empty deferred message type
	ErrScheduleMessageTypeRequired = errors.New(
		"schedule message type is required",
	)
)

var scheduleAppliers = Appliers[scheduleState]{
	scheduleChanged:  applyScheduleChanged,
	scheduleConsumed: clearSchedule,
}

// Schedule creates or replaces a durable deferred message. Scheduling the same
// deadline and message again is a no-op
func (t *Transaction) Schedule(
	key ScheduleKey, at time.Time, message *Message,
) error {
	if err := validateSchedule(key, message); err != nil {
		return err
	}
	id := makeScheduleID(key)
	changed := false
	_, err := t.execInternal(t.store.schedule, id,
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			if matchesSchedule(st.Active, at, message) {
				return nil
			}
			changed = true
			return ag.Raise(scheduleChanged, &Schedule{
				Message: message,
				At:      at.UTC(),
				Key:     key,
			})
		},
	)
	if err != nil || !changed {
		return err
	}
	t.setScheduleStatus(id, scheduleActiveStatus, at)
	return nil
}

// ConsumeSchedule removes and returns the schedule for key if it is due by
// through. It returns nil when nothing is due
func (t *Transaction) ConsumeSchedule(
	key ScheduleKey, through time.Time,
) (*Schedule, error) {
	if key == "" {
		return nil, ErrScheduleKeyRequired
	}
	id := makeScheduleID(key)
	var res *Schedule
	_, err := t.execInternal(t.store.schedule, id,
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			if st.Active == nil || st.Active.At.After(through) {
				return nil
			}
			res = st.Active
			return ag.Raise(scheduleConsumed, Empty{})
		},
	)
	if err != nil || res == nil {
		return nil, err
	}
	t.parts[id].request.Delete = true
	return res, nil
}

// ScheduleChanges reports coalesced local schedule commit notifications
func (s *Store) ScheduleChanges() <-chan struct{} {
	return s.changes
}

// TakeScheduleChanges returns and clears the schedules this Store committed
// since the previous call, with nil marking a consumed key. Changes are only
// collected after the first call
func (s *Store) TakeScheduleChanges() map[ScheduleKey]*Schedule {
	s.changedMu.Lock()
	defer s.changedMu.Unlock()
	res := s.changed
	s.changed = map[ScheduleKey]*Schedule{}
	return res
}

// LoadSchedule loads the active schedule for a key
func (s *Store) LoadSchedule(key ScheduleKey) (*Schedule, error) {
	if key == "" {
		return nil, ErrScheduleKeyRequired
	}
	proj, _, err := s.schedule.readProjection(makeScheduleID(key))
	if err != nil {
		return nil, err
	}
	return proj.state.Active, nil
}

// ListSchedules lists active schedules through the provided time, or all
// schedules when through is zero
func (s *Store) ListSchedules(through time.Time) ([]*Schedule, error) {
	entries, err := s.backend.ListAggregatesByStatus(StatusQuery{
		Status:  scheduleActiveStatus,
		Type:    scheduleAggregateType,
		Through: through,
	})
	if err != nil {
		return nil, err
	}
	res := make([]*Schedule, 0, len(entries))
	for _, entry := range entries {
		proj, _, err := s.schedule.readProjection(entry.ID)
		if err != nil {
			return nil, err
		}
		if proj.state.Active == nil {
			continue
		}
		res = append(res, proj.state.Active)
	}
	return res, nil
}

func (t *Transaction) setScheduleStatus(
	id AggregateID, status string, at time.Time,
) {
	p := t.parts[id]
	p.request.Status = &status
	p.request.StatusAt = at.UTC()
	p.request.Delete = false
}

func (s *Store) notifyScheduleChange(st scheduleState, evs []*Event) {
	if len(evs) == 0 {
		return
	}
	s.changedMu.Lock()
	if s.changed != nil {
		key := ScheduleKey(evs[0].AggregateID.Key)
		s.changed[key] = st.Active
	}
	s.changedMu.Unlock()
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

func newScheduleState() scheduleState {
	return scheduleState{}
}

func applyScheduleChanged(st scheduleState, ev *Event) scheduleState {
	schedule, err := ev.GetValue[*Schedule]()
	if err != nil {
		return st
	}
	st.Active = schedule
	return st
}

func clearSchedule(st scheduleState, _ *Event) scheduleState {
	st.Active = nil
	return st
}

func makeScheduleID(key ScheduleKey) AggregateID {
	return NewAggregateID(scheduleAggregateType, ID(key))
}

func validateSchedule(key ScheduleKey, message *Message) error {
	if key == "" {
		return ErrScheduleKeyRequired
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

func matchesSchedule(s *Schedule, at time.Time, msg *Message) bool {
	return s != nil && s.At.Equal(at) && s.Message.Equal(msg)
}

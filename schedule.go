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
		Version ScheduleVersion
	}

	// ScheduleVersionConflictError indicates a stale schedule incarnation
	ScheduleVersionConflictError struct {
		Key             ScheduleKey
		ExpectedVersion ScheduleVersion
	}

	// ScheduleKey identifies one replaceable deferred message
	ScheduleKey string

	// ScheduleVersion identifies one incarnation within a schedule aggregate
	ScheduleVersion int64

	scheduleState struct {
		Active *Schedule `json:"active,omitempty"`
	}
)

const (
	scheduleAggregateType ID        = "_tb.sched_"
	scheduleChanged       EventType = "_tb.sched.changed"
	scheduleCanceled      EventType = "_tb.sched.canceled"
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
	scheduleCanceled: clearSchedule,
	scheduleConsumed: clearSchedule,
}

// Schedule creates or replaces a durable deferred message
func (t *Transaction) Schedule(
	key ScheduleKey, at time.Time, message *Message,
) error {
	if err := validateSchedule(key, message); err != nil {
		return err
	}
	id := makeScheduleID(key)
	_, err := t.Exec(t.store.schedule, id,
		func(_ scheduleState, ag *Aggregator[scheduleState]) error {
			ver := ScheduleVersion(ag.NextSequence())
			return ag.Raise(scheduleChanged, &Schedule{
				Message: cloneMessage(message),
				At:      at.UTC(),
				Key:     key,
				Version: ver,
			})
		},
	)
	if err != nil {
		return err
	}
	t.setScheduleStatus(id, scheduleActiveStatus, at)
	return nil
}

// CancelSchedule removes the current schedule for a key
func (t *Transaction) CancelSchedule(key ScheduleKey) error {
	if key == "" {
		return ErrScheduleKeyRequired
	}
	id := makeScheduleID(key)
	changed := false
	_, err := t.Exec(t.store.schedule, id,
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			if st.Active == nil {
				return nil
			}
			changed = true
			return ag.Raise(scheduleCanceled, st.Active.Version)
		},
	)
	if err != nil || !changed {
		return err
	}
	t.setScheduleStatus(id, "", time.Time{})
	return nil
}

// CancelSchedulePrefix removes active schedules whose keys share prefix
func (t *Transaction) CancelSchedulePrefix(prefix ScheduleKey) error {
	if prefix == "" {
		return ErrScheduleKeyRequired
	}
	entries, err := t.store.backend.ListAggregatesByStatus(StatusQuery{
		Status:    scheduleActiveStatus,
		Type:      scheduleAggregateType,
		KeyPrefix: ID(prefix),
	})
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := t.CancelSchedule(ScheduleKey(entry.ID.Key)); err != nil {
			return err
		}
	}
	return nil
}

// ConsumeSchedule conditionally removes one observed schedule incarnation
func (t *Transaction) ConsumeSchedule(
	key ScheduleKey, version ScheduleVersion,
) error {
	if key == "" {
		return ErrScheduleKeyRequired
	}
	id := makeScheduleID(key)
	_, err := t.Exec(t.store.schedule, id,
		func(st scheduleState, ag *Aggregator[scheduleState]) error {
			if st.Active == nil || st.Active.Version != version {
				return &ScheduleVersionConflictError{
					Key:             key,
					ExpectedVersion: version,
				}
			}
			return ag.Raise(scheduleConsumed, version)
		},
	)
	if err != nil {
		return err
	}
	t.setScheduleStatus(id, "", time.Time{})
	return nil
}

// ScheduleChanges reports coalesced local schedule commit notifications
func (s *Store) ScheduleChanges() <-chan struct{} {
	return s.changes
}

// LoadSchedule loads the active schedule for a key
func (s *Store) LoadSchedule(key ScheduleKey) (*Schedule, error) {
	if key == "" {
		return nil, ErrScheduleKeyRequired
	}
	st, err := s.schedule.loadState(makeScheduleID(key))
	if err != nil {
		return nil, err
	}
	if st.Active == nil {
		return nil, nil
	}
	return cloneSchedule(st.Active), nil
}

// ListSchedules lists active schedules through the provided time, or all
// schedules when through is zero
func (s *Store) ListSchedules(through time.Time) ([]*Schedule, error) {
	entries, err := s.backend.ListAggregatesByStatus(StatusQuery{
		Status:  scheduleActiveStatus,
		Through: through,
	})
	if err != nil {
		return nil, err
	}
	res := make([]*Schedule, 0, len(entries))
	for _, entry := range entries {
		if entry.ID.Type != scheduleAggregateType {
			continue
		}
		st, err := s.schedule.loadState(entry.ID)
		if err != nil {
			return nil, err
		}
		if st.Active == nil {
			continue
		}
		res = append(res, cloneSchedule(st.Active))
	}
	return res, nil
}

// Error describes a stale schedule incarnation
func (s *ScheduleVersionConflictError) Error() string {
	return fmt.Sprintf("schedule version conflict for %q: expected %d",
		s.Key, s.ExpectedVersion)
}

func (t *Transaction) setScheduleStatus(
	id AggregateID, status string, at time.Time,
) {
	p := t.parts[id]
	p.request.Status = &status
	p.request.StatusAt = at.UTC()
}

func newScheduleState() scheduleState {
	return scheduleState{}
}

func applyScheduleChanged(st scheduleState, ev *Event) scheduleState {
	schedule, err := ev.GetValue[*Schedule]()
	if err != nil {
		return st
	}
	st.Active = cloneSchedule(schedule)
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

func cloneSchedule(schedule *Schedule) *Schedule {
	if schedule == nil {
		return nil
	}
	return &Schedule{
		Message: cloneMessage(schedule.Message),
		At:      schedule.At,
		Key:     schedule.Key,
		Version: schedule.Version,
	}
}

func cloneMessage(message *Message) *Message {
	if message == nil {
		return nil
	}
	return &Message{
		Type:        message.Type,
		AggregateID: message.AggregateID,
		Data:        append([]byte(nil), message.Data...),
	}
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

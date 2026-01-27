package timebox

import (
	"encoding/json"
	"strings"
	"time"
	"unsafe"
)

type (
	// Aggregator maintains aggregate state and tracks events raised during a
	// command. It is not safe for concurrent use
	Aggregator[T any] struct {
		value    T
		appliers Appliers[T]
		id       AggregateID
		enqueued []*Event
		nextSeq  int64
		success  []SuccessAction[T]
	}

	// Flusher persists enqueued events and returns an error if the write fails
	Flusher func(int64, []*Event) error

	// SuccessAction receives the Aggregator's final value upon Exec success
	SuccessAction[T any] func(T)

	// AggregateID identifies an aggregate as a set of parts ("order", "123")
	AggregateID []ID

	// ID is a single component of an AggregateID
	ID string
)

func newAggregator[T any](
	id AggregateID, appliers Appliers[T], initValue T, initSeq int64,
) *Aggregator[T] {
	return &Aggregator[T]{
		id:       id,
		nextSeq:  initSeq,
		enqueued: []*Event{},
		appliers: appliers,
		value:    initValue,
	}
}

// ID returns the aggregate's identifier components
func (a *Aggregator[_]) ID() AggregateID {
	return a.id
}

// Value returns the aggregate's current state
func (a *Aggregator[T]) Value() T {
	return a.value
}

// NextSequence returns the next sequence number that will be assigned to a new
// event
func (a *Aggregator[_]) NextSequence() int64 {
	return a.nextSeq
}

// Enqueued returns the events raised during the current command
func (a *Aggregator[_]) Enqueued() []*Event {
	return a.enqueued
}

// OnSuccess registers an action to run if the executor completes without error
func (a *Aggregator[T]) OnSuccess(fn SuccessAction[T]) {
	a.success = append(a.success, fn)
}

func (a *Aggregator[T]) raise(typ EventType, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	ev := &Event{
		Timestamp:   time.Now(),
		Sequence:    a.nextSeq,
		AggregateID: a.id,
		Type:        typ,
		Data:        data,
		value:       value,
	}
	a.enqueued = append(a.enqueued, ev)
	a.nextSeq++
	a.Apply(ev)
	return nil
}

// Apply updates the aggregate state using the applier for the event
func (a *Aggregator[T]) Apply(ev *Event) {
	if apply, ok := a.appliers[ev.Type]; ok {
		a.value = apply(a.value, ev)
	}
}

// Flush writes enqueued events through the provided flusher and clears the
// queue on success
func (a *Aggregator[_]) Flush(f Flusher) (int, error) {
	count := len(a.enqueued)
	if count == 0 {
		return 0, nil
	}
	expectedSeq := a.nextSeq - int64(count)
	if err := f(expectedSeq, a.enqueued); err != nil {
		return count, err
	}
	a.enqueued = []*Event{}
	return count, nil
}

func NewAggregateID(parts ...ID) AggregateID {
	return parts
}

// ParseAggregateID splits a string by the separator into an AggregateID
func ParseAggregateID(str, sep string) AggregateID {
	s := strings.Split(str, sep)
	return *(*AggregateID)(unsafe.Pointer(&s))
}

// Join combines the AggregateID parts into a single string using a separator
func (id AggregateID) Join(sep string) string {
	s := *(*[]string)(unsafe.Pointer(&id))
	return strings.Join(s, sep)
}

// Equal compares two AggregateIDs for equality
func (id AggregateID) Equal(other AggregateID) bool {
	if len(id) != len(other) {
		return false
	}
	for i, p := range id {
		if other[i] != p {
			return false
		}
	}
	return true
}

// Raise marshals the value and enqueues a new event on the Aggregator
func Raise[T, V any](ag *Aggregator[T], typ EventType, value V) error {
	return ag.raise(typ, value)
}

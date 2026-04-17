package timebox

import (
	"encoding/json"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"
)

type (
	// Aggregator maintains aggregate state for a command and tracks events
	// raised through Raise. It is not safe for concurrent use
	Aggregator[T any] struct {
		value    T
		appliers Appliers[T]
		id       AggregateID
		enqueued []*Event
		flushed  []*Event
		nextSeq  int64
		success  []SuccessAction[T]
	}

	// Appliers is a map of EventType to Applier for a given aggregate
	Appliers[T any] map[EventType]Applier[T]

	// Applier applies an event to an aggregate state, returning the new state
	Applier[T any] func(T, *Event) T

	// Flusher persists enqueued events and returns an error if the write fails
	Flusher func(int64, []*Event) error

	// SuccessAction receives the Aggregator's final value after Executor.Exec
	// succeeds, as well as the Events persisted by that execution
	SuccessAction[T any] func(T, []*Event)

	// AggregateID identifies an aggregate as a set of parts ("order", "123")
	AggregateID []ID

	// ID is a single component of an AggregateID
	ID string
)

// NewAggregateID builds an AggregateID from its parts
func NewAggregateID(parts ...ID) AggregateID {
	return parts
}

// Raise marshals the value and enqueues a new event on the Aggregator
func Raise[T, V any](ag *Aggregator[T], typ EventType, value V) error {
	return ag.raise(typ, value)
}

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

// OnSuccess registers an action to run after Executor.Exec persists the raised
// events successfully
func (a *Aggregator[T]) OnSuccess(fn SuccessAction[T]) {
	a.success = append(a.success, fn)
}

func (a *Aggregator[T]) apply(ev *Event) {
	if apply, ok := a.appliers[ev.Type]; ok {
		a.value = apply(a.value, ev)
	}
}

func (a *Aggregator[_]) flush(f Flusher) (int, error) {
	count := len(a.enqueued)
	expectedSeq := a.nextSeq - int64(count)
	if err := f(expectedSeq, a.enqueued); err != nil {
		return count, err
	}
	if count == 0 {
		return 0, nil
	}
	if len(a.flushed) == 0 {
		a.flushed = a.enqueued
	} else {
		a.flushed = slices.Concat(a.flushed, a.enqueued)
	}
	a.enqueued = []*Event{}
	return count, nil
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
		Raised:      true,
		value:       value,
	}
	a.enqueued = append(a.enqueued, ev)
	a.nextSeq++
	a.apply(ev)
	return nil
}

func (a *Aggregator[T]) runOnSuccess(defaults []SuccessAction[T]) {
	val := a.value
	evs := a.flushed
	for _, fn := range combineSuccess(defaults, a.success) {
		func(cb SuccessAction[T]) {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("OnSuccess action panicked",
						slog.Any("aggregate_id", a.id),
						slog.Any("panic", r))
				}
			}()
			cb(val, evs)
		}(fn)
	}
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

// String returns a human-readable AggregateID representation
func (id AggregateID) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, p := range id {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(string(p)))
	}
	b.WriteByte(']')
	return b.String()
}

// HasPrefix checks if the AggregateID starts with the provided prefix
func (id AggregateID) HasPrefix(prefix AggregateID) bool {
	if len(prefix) > len(id) {
		return false
	}
	for i, p := range prefix {
		if id[i] != p {
			return false
		}
	}
	return true
}

func combineSuccess[T any](def, agg []SuccessAction[T]) []SuccessAction[T] {
	switch {
	case len(def) == 0:
		return agg
	case len(agg) == 0:
		return def
	default:
		return slices.Concat(def, agg)
	}
}

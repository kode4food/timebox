package timebox

import (
	"encoding/json"
	"errors"
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
		tx       *Transaction
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

	// AggregateID identifies an aggregate by type and key ("order", "123")
	AggregateID struct {
		Type ID
		Key  ID
	}

	// ID is a single component of an AggregateID
	ID string
)

// SingletonKey is the Key of an aggregate that is the only one of its type
const SingletonKey ID = "_"

var (
	// ErrInvalidAggregateID indicates an encoded AggregateID did not decode to
	// a type and a key
	ErrInvalidAggregateID = errors.New(
		"aggregate id must have a type and a key",
	)
)

// NewAggregateID builds an AggregateID from its type and key
func NewAggregateID(typ, key ID) AggregateID {
	return AggregateID{Type: typ, Key: key}
}

// NewAggregateType builds the AggregateID of the only aggregate of a type
func NewAggregateType(typ ID) AggregateID {
	return AggregateID{Type: typ, Key: SingletonKey}
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

// ID returns the aggregate's identifier
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

// Raise marshals the value and enqueues a new event on the Aggregator
func (a *Aggregator[T]) Raise[V any](typ EventType, value V) error {
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

// Transaction returns the Transaction this Aggregator's events commit in, so
// code holding only an Aggregator can enlist further aggregates
func (a *Aggregator[_]) Transaction() *Transaction {
	return a.tx
}

// String returns a human-readable AggregateID representation
func (id AggregateID) String() string {
	var b strings.Builder
	b.WriteByte('[')
	b.WriteString(strconv.Quote(string(id.Type)))
	b.WriteByte(',')
	b.WriteString(strconv.Quote(string(id.Key)))
	b.WriteByte(']')
	return b.String()
}

// MarshalJSON encodes the AggregateID as a type and key pair
func (id AggregateID) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]ID{id.Type, id.Key})
}

// UnmarshalJSON decodes the AggregateID from a type and key pair
func (id *AggregateID) UnmarshalJSON(data []byte) error {
	var parts []ID
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	if len(parts) != 2 {
		return ErrInvalidAggregateID
	}
	*id = NewAggregateID(parts[0], parts[1])
	return nil
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

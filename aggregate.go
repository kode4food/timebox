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

	// AggregateID identifies an aggregate by type and key ("order", "123"). An
	// empty Key names the type itself
	AggregateID struct {
		Type ID
		Key  ID
	}

	// ID is a single component of an AggregateID
	ID string
)

var (
	// ErrInvalidAggregateID indicates an encoded AggregateID did not decode to
	// a type and an optional key
	ErrInvalidAggregateID = errors.New(
		"aggregate id must have at most a type and a key",
	)
)

// NewAggregateID builds an AggregateID from its type and key
func NewAggregateID(typ, key ID) AggregateID {
	return AggregateID{Type: typ, Key: key}
}

// NewAggregateType builds an AggregateID naming a type but no individual
// aggregate, matching every aggregate of that type when used as a prefix
func NewAggregateType(typ ID) AggregateID {
	return AggregateID{Type: typ}
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

// Parts returns the AggregateID's populated components, from its type to its
// key. A zero AggregateID has no parts
func (id AggregateID) Parts() []ID {
	switch {
	case id.Type == "":
		return nil
	case id.Key == "":
		return []ID{id.Type}
	default:
		return []ID{id.Type, id.Key}
	}
}

// String returns a human-readable AggregateID representation
func (id AggregateID) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, p := range id.Parts() {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(string(p)))
	}
	b.WriteByte(']')
	return b.String()
}

// HasPrefix checks if the AggregateID starts with the provided prefix. A prefix
// naming only a type matches every aggregate of that type, and the zero
// AggregateID matches everything
func (id AggregateID) HasPrefix(prefix AggregateID) bool {
	switch {
	case prefix.Type == "":
		return true
	case prefix.Type != id.Type:
		return false
	default:
		return prefix.Key == "" || prefix.Key == id.Key
	}
}

// MarshalJSON encodes the AggregateID as an array of its parts
func (id AggregateID) MarshalJSON() ([]byte, error) {
	return json.Marshal(id.Parts())
}

// UnmarshalJSON decodes the AggregateID from an array of its parts
func (id *AggregateID) UnmarshalJSON(data []byte) error {
	var parts []ID
	if err := json.Unmarshal(data, &parts); err != nil {
		return err
	}
	res, err := AggregateIDFromParts(parts)
	if err != nil {
		return err
	}
	*id = res
	return nil
}

// AggregateIDFromParts rebuilds an AggregateID from parts decoded out of
// storage or off the wire
func AggregateIDFromParts[T ~string](parts []T) (AggregateID, error) {
	switch len(parts) {
	case 0:
		return AggregateID{}, nil
	case 1:
		return NewAggregateType(ID(parts[0])), nil
	case 2:
		return NewAggregateID(ID(parts[0]), ID(parts[1])), nil
	default:
		return AggregateID{}, ErrInvalidAggregateID
	}
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

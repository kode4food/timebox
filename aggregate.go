package timebox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type (
	Aggregator[T any] struct {
		value    T
		appliers map[EventType]Applier[T]
		id       AggregateID
		enqueued []*Event
		next     int64
	}

	Applier[T any] func(T, *Event) T

	Flusher func([]*Event) error
)

func newAggregator[T any](
	id AggregateID, appliers map[EventType]Applier[T], initialValue T, initialSeq int64,
) *Aggregator[T] {
	return &Aggregator[T]{
		id:       id,
		next:     initialSeq,
		enqueued: []*Event{},
		appliers: appliers,
		value:    initialValue,
	}
}

// NewAggregator creates a new aggregator (for testing)
func NewAggregator[T any](
	id AggregateID, appliers map[EventType]Applier[T], initialValue T,
) *Aggregator[T] {
	return newAggregator(id, appliers, initialValue, 0)
}

func (a *Aggregator[_]) ID() AggregateID {
	return a.id
}

func (a *Aggregator[T]) Value() T {
	return a.value
}

func (a *Aggregator[_]) Enqueued() []*Event {
	return a.enqueued
}

func (a *Aggregator[T]) Raise(typ EventType, data json.RawMessage) {
	ev := &Event{
		ID:          ID(uuid.New().String()),
		Timestamp:   time.Now(),
		AggregateID: a.id,
		Type:        typ,
		Sequence:    a.next,
		Data:        data,
	}
	a.enqueued = append(a.enqueued, ev)
	a.next++
	a.Apply(ev)
}

func (a *Aggregator[T]) Apply(ev *Event) {
	if apply, ok := a.appliers[ev.Type]; ok {
		a.value = apply(a.value, ev)
	}
}

func (a *Aggregator[_]) Flush(f Flusher) (int, error) {
	count := len(a.enqueued)
	if count == 0 {
		return 0, nil
	}
	if err := f(a.enqueued); err != nil {
		return count, err
	}
	a.next = a.enqueued[count-1].Sequence + 1
	a.enqueued = []*Event{}
	return count, nil
}

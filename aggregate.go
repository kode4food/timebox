package timebox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type (
	Aggregator[T any] struct {
		value    T
		appliers Appliers[T]
		id       AggregateID
		enqueued []*Event
		next     int64
	}

	Appliers[T any] map[EventType]Applier[T]

	Applier[T any] func(T, *Event) T
	Flusher        func(int64, []*Event) error
)

func newAggregator[T any](
	id AggregateID, appliers Appliers[T], initValue T, initSeq int64,
) *Aggregator[T] {
	return &Aggregator[T]{
		id:       id,
		next:     initSeq,
		enqueued: []*Event{},
		appliers: appliers,
		value:    initValue,
	}
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
	expectedSeq := a.next - int64(count)
	if err := f(expectedSeq, a.enqueued); err != nil {
		return count, err
	}
	a.enqueued = []*Event{}
	return count, nil
}

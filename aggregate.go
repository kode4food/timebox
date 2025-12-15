package timebox

import (
	"encoding/json"
	"strings"
	"time"
	"unsafe"
)

type (
	Aggregator[T any] struct {
		value    T
		appliers Appliers[T]
		id       AggregateID
		enqueued []*Event
		nextSeq  int64
	}

	Flusher func(int64, []*Event) error

	AggregateID []ID
	ID          string
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

func (a *Aggregator[_]) ID() AggregateID {
	return a.id
}

func (a *Aggregator[T]) Value() T {
	return a.value
}

func (a *Aggregator[_]) NextSequence() int64 {
	return a.nextSeq
}

func (a *Aggregator[_]) Enqueued() []*Event {
	return a.enqueued
}

func (a *Aggregator[T]) Raise(typ EventType, value any) error {
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

func ParseAggregateID(str, sep string) AggregateID {
	s := strings.Split(str, sep)
	return *(*AggregateID)(unsafe.Pointer(&s))
}

func (id AggregateID) Join(sep string) string {
	s := *(*[]string)(unsafe.Pointer(&id))
	return strings.Join(s, sep)
}

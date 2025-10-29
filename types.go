package timebox

import (
	"encoding/json"
	"strings"
	"time"
	"unsafe"
)

type (
	ID          string
	EventType   string
	AggregateID []ID

	Event struct {
		Timestamp   time.Time       `json:"timestamp"`
		ID          ID              `json:"id"`
		Type        EventType       `json:"type"`
		AggregateID AggregateID     `json:"aggregate_id"`
		Data        json.RawMessage `json:"data"`
		Sequence    int64           `json:"sequence"`
	}

	// projection is internal wrapper that tracks sequence alongside user state
	projection[T any] struct {
		State        T     `json:"state"`
		NextSequence int64 `json:"next_sequence"`
	}
)

func (p *projection[T]) GetNextSequence() int64 {
	return p.NextSequence
}

func ParseAggregateID(str, sep string) AggregateID {
	s := strings.Split(str, sep)
	return *(*AggregateID)(unsafe.Pointer(&s))
}

func NewAggregateID(parts ...ID) AggregateID {
	return parts
}

func (id AggregateID) Join(sep string) string {
	s := *(*[]string)(unsafe.Pointer(&id))
	return strings.Join(s, sep)
}

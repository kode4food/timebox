package timebox

import (
	"encoding/json"
	"strings"
	"time"
	"unsafe"

	"github.com/kode4food/caravan/topic"
)

type (
	ID          string
	EventType   string
	AggregateID []ID

	EventHub topic.Topic[*Event]

	Event struct {
		Timestamp   time.Time       `json:"timestamp"`
		ID          ID              `json:"id"`
		Type        EventType       `json:"type"`
		AggregateID AggregateID     `json:"aggregate_id"`
		Data        json.RawMessage `json:"data"`
		Sequence    int64           `json:"sequence"`
	}

	projection[T any] struct {
		state   T
		nextSeq int64
	}
)

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

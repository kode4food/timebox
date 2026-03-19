package timebox

import (
	"encoding/json"
	"time"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	// EventCodec encodes and decodes complete events
	EventCodec[T any] interface {
		Encode(*Event) (T, error)
		Decode(T) (*Event, error)
	}

	jsonCodec struct{}
	binCodec  struct{}
)

var (
	// JSONEvent encodes complete events as JSON
	JSONEvent EventCodec[[]byte] = jsonCodec{}

	// BinEvent encodes complete events using the internal binary format
	BinEvent EventCodec[[]byte] = binCodec{}
)

// Encode converts an Event to JSON using its struct tags
func (jsonCodec) Encode(ev *Event) ([]byte, error) {
	return json.Marshal(ev)
}

// Decode converts JSON into an Event using its struct tags
func (jsonCodec) Decode(data []byte) (*Event, error) {
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}

// Encode converts an Event to the internal binary format
func (binCodec) Encode(ev *Event) ([]byte, error) {
	buf := make([]byte, 0, 64+len(ev.Data))
	buf = bin.AppendInt64(buf, ev.Timestamp.UnixNano())
	buf = bin.AppendInt64(buf, ev.Sequence)
	buf = appendAggregateID(buf, ev.AggregateID)
	buf = bin.AppendString(buf, string(ev.Type))
	buf = bin.AppendBytes(buf, ev.Data)
	return buf, nil
}

// Decode converts the internal binary format into an Event
func (binCodec) Decode(data []byte) (*Event, error) {
	ts, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	seq, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	id, data, err := readAggregateID(data)
	if err != nil {
		return nil, err
	}
	typ, data, err := bin.ReadString(data)
	if err != nil {
		return nil, err
	}
	payload, data, err := bin.ReadBytes(data)
	if err != nil {
		return nil, err
	}
	if len(data) != 0 {
		return nil, bin.ErrCorruptState
	}
	return &Event{
		Timestamp:   time.Unix(0, ts).UTC(),
		Sequence:    seq,
		Type:        EventType(typ),
		AggregateID: id,
		Data:        payload,
	}, nil
}

func appendAggregateID(buf []byte, id AggregateID) []byte {
	buf = bin.AppendUint32(buf, uint32(len(id)))
	for _, part := range id {
		buf = bin.AppendString(buf, string(part))
	}
	return buf
}

func readAggregateID(data []byte) (AggregateID, []byte, error) {
	n, data, err := bin.ReadUint32(data)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, data, nil
	}
	id := make(AggregateID, n)
	for i := range id {
		part, rest, err := bin.ReadString(data)
		if err != nil {
			return nil, nil, err
		}
		id[i] = ID(part)
		data = rest
	}
	return id, data, nil
}

package timebox

import (
	"encoding/json"
	"time"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	// Codec encodes and decodes a value to and from its representation
	Codec[Value any, Repr any] interface {
		Encode(Value) (Repr, error)
		Decode(Repr) (Value, error)
	}

	// EventCodec encodes and decodes complete events
	EventCodec[Repr any] interface {
		Codec[*Event, Repr]
	}

	jsonCodec struct{}
	binCodec  struct{}
)

var (
	// JSONEvent encodes complete events as JSON
	JSONEvent EventCodec[string] = jsonCodec{}

	// BinEvent encodes complete events using the internal binary format
	BinEvent EventCodec[[]byte] = binCodec{}

	// EncodeJSONEvents encodes complete events as JSON strings
	EncodeJSONEvents = MakeEncodeAll(JSONEvent)

	// DecodeJSONEvents decodes complete events from JSON strings
	DecodeJSONEvents = MakeDecodeAll(JSONEvent)

	// EncodeBinEvents encodes complete events to the internal binary format
	EncodeBinEvents = MakeEncodeAll(BinEvent)

	// DecodeBinEvents decodes complete events from the internal binary format
	DecodeBinEvents = MakeDecodeAll(BinEvent)
)

// MakeEncodeAll returns a batch encoder for the provided codec
func MakeEncodeAll[Value any, Repr any](
	codec Codec[Value, Repr],
) func([]Value) ([]Repr, error) {
	return func(vals []Value) ([]Repr, error) {
		res := make([]Repr, 0, len(vals))
		for _, val := range vals {
			repr, err := codec.Encode(val)
			if err != nil {
				return nil, err
			}
			res = append(res, repr)
		}
		return res, nil
	}
}

// MakeDecodeAll returns a batch decoder for the provided codec
func MakeDecodeAll[Value any, Repr any](
	codec Codec[Value, Repr],
) func([]Repr) ([]Value, error) {
	return func(vals []Repr) ([]Value, error) {
		res := make([]Value, 0, len(vals))
		for _, val := range vals {
			decoded, err := codec.Decode(val)
			if err != nil {
				return nil, err
			}
			res = append(res, decoded)
		}
		return res, nil
	}
}

// Encode converts an Event to JSON using its struct tags
func (jsonCodec) Encode(ev *Event) (string, error) {
	data, err := json.Marshal(ev)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Decode converts JSON into an Event using its struct tags
func (jsonCodec) Decode(data string) (*Event, error) {
	var ev Event
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
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

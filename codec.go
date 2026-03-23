package timebox

import (
	"encoding/json"
	"time"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	// Codec encodes and decodes values
	Codec[Value any] interface {
		// Encode encodes a value to bytes
		Encode(Value) ([]byte, error)

		// Decode decodes bytes to a value
		Decode([]byte) (Value, error)

		// Append appends an encoded value to a buffer
		Append([]byte, Value) ([]byte, error)

		// Read reads a value from a buffer and returns the remainder
		Read([]byte) (Value, []byte, error)
	}

	// EventCodec encodes and decodes complete events
	EventCodec interface {
		Codec[*Event]
	}

	jsonEventCodec struct{}
	binEventCodec  struct{}
)

var (
	// JSONEvent encodes complete events as JSON
	JSONEvent EventCodec = jsonEventCodec{}

	// BinEvent encodes complete events using the internal binary format
	BinEvent EventCodec = binEventCodec{}

	// EncodeJSONEvents encodes complete events as JSON bytes
	EncodeJSONEvents = MakeEncodeAll(JSONEvent)

	// DecodeJSONEvents decodes complete events from JSON bytes
	DecodeJSONEvents = MakeDecodeAll(JSONEvent)

	// EncodeBinEvents encodes complete events to the internal binary format
	EncodeBinEvents = MakeEncodeAll(BinEvent)

	// DecodeBinEvents decodes complete events from the internal binary format
	DecodeBinEvents = MakeDecodeAll(BinEvent)
)

// MakeEncodeAll returns a batch encoder for the provided codec
func MakeEncodeAll[Value any](
	codec Codec[Value],
) func([]Value) ([][]byte, error) {
	return func(vals []Value) ([][]byte, error) {
		res := make([][]byte, 0, len(vals))
		for _, val := range vals {
			b, err := codec.Encode(val)
			if err != nil {
				return nil, err
			}
			res = append(res, b)
		}
		return res, nil
	}
}

// MakeDecodeAll returns a batch decoder for the provided codec
func MakeDecodeAll[Value any](
	codec Codec[Value],
) func([][]byte) ([]Value, error) {
	return func(vals [][]byte) ([]Value, error) {
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
func (c jsonEventCodec) Encode(ev *Event) ([]byte, error) {
	return c.Append(nil, ev)
}

// Decode converts JSON into an Event using its struct tags
func (c jsonEventCodec) Decode(data []byte) (*Event, error) {
	res, _, err := c.Read(data)
	return res, err
}

// Append appends a JSON-encoded Event to a buffer
func (jsonEventCodec) Append(buf []byte, ev *Event) ([]byte, error) {
	data, err := json.Marshal(ev)
	if err != nil {
		return nil, err
	}
	return append(buf, data...), nil
}

// Read reads and decodes a JSON Event from a buffer
func (jsonEventCodec) Read(data []byte) (*Event, []byte, error) {
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, nil, err
	}
	return &ev, nil, nil
}

// Encode converts an Event to the internal binary format
func (c binEventCodec) Encode(ev *Event) ([]byte, error) {
	return c.Append(nil, ev)
}

// Decode converts the internal binary format into an Event
func (c binEventCodec) Decode(data []byte) (*Event, error) {
	res, _, err := c.Read(data)
	return res, err
}

// Append appends a binary-encoded Event to a buffer
func (binEventCodec) Append(buf []byte, ev *Event) ([]byte, error) {
	buf = bin.AppendInt64(buf, ev.Timestamp.UnixNano())
	buf = bin.AppendInt64(buf, ev.Sequence)
	buf = appendAggregateID(buf, ev.AggregateID)
	buf = bin.AppendString(buf, string(ev.Type))
	buf = bin.AppendBytes(buf, ev.Data)
	return buf, nil
}

// Read reads and decodes a binary Event from a buffer
func (binEventCodec) Read(data []byte) (*Event, []byte, error) {
	ts, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, nil, err
	}
	seq, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, nil, err
	}
	id, data, err := readAggregateID(data)
	if err != nil {
		return nil, nil, err
	}
	typ, data, err := bin.ReadString(data)
	if err != nil {
		return nil, nil, err
	}
	payload, data, err := bin.ReadBytes(data)
	if err != nil {
		return nil, nil, err
	}
	return &Event{
		Timestamp:   time.Unix(0, ts).UTC(),
		Sequence:    seq,
		Type:        EventType(typ),
		AggregateID: id,
		Data:        payload,
	}, data, nil
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

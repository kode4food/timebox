package timebox

import (
	"encoding/json"
	"time"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	// Codec encodes and decodes values
	Codec[Value, Data any] interface {
		// Encode encodes a value to the codec data format
		Encode(Value) (Data, error)

		// Decode decodes codec data to a value
		Decode(Data) (Value, error)

		// Append appends an encoded value to a buffer
		Append(Data, Value) (Data, error)

		// Read reads a value from a buffer and returns the remainder
		Read(Data) (Value, Data, error)
	}

	// EventCodec encodes and decodes complete events
	EventCodec[Data any] interface {
		Codec[*Event, Data]
	}

	jsonEventCodec struct{}
	binEventCodec  struct{}
)

var (
	// JSONEvent encodes complete events as JSON
	JSONEvent EventCodec[[]byte] = jsonEventCodec{}

	// BinEvent encodes complete events using the internal binary format
	BinEvent interface {
		EventCodec[[]byte]
		AppendAll(buf []byte, evs []*Event) ([]byte, error)
		ReadAll(data []byte) ([]*Event, []byte, error)
	} = binEventCodec{}

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
func MakeEncodeAll[Value, Data any](
	codec Codec[Value, Data],
) func([]Value) ([]Data, error) {
	return func(vals []Value) ([]Data, error) {
		res := make([]Data, 0, len(vals))
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
func MakeDecodeAll[Value, Data any](
	codec Codec[Value, Data],
) func([]Data) ([]Value, error) {
	return func(vals []Data) ([]Value, error) {
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

// AppendAll appends a length-prefixed batch of binary-encoded Events
func (c binEventCodec) AppendAll(buf []byte, evs []*Event) ([]byte, error) {
	buf = bin.AppendUint32(buf, uint32(len(evs)))
	for _, ev := range evs {
		var err error
		buf, err = c.Append(buf, ev)
		if err != nil {
			return buf, err
		}
	}
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

// ReadAll reads a length-prefixed batch of binary-encoded Events
func (c binEventCodec) ReadAll(data []byte) ([]*Event, []byte, error) {
	n, data, err := bin.ReadUint32(data)
	if err != nil {
		return nil, nil, err
	}
	res := make([]*Event, n)
	for i := range res {
		res[i], data, err = c.Read(data)
		if err != nil {
			return nil, data, err
		}
	}
	return res, data, nil
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

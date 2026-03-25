package timebox_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	bin "github.com/kode4food/timebox/internal/binary"
)

func TestJSONCodec(t *testing.T) {
	codec := timebox.JSONEvent

	ev := codecEvent()
	data, err := codec.Encode(ev)
	assert.NoError(t, err)

	got, err := codec.Decode(data)
	assert.NoError(t, err)
	assert.Equal(t, ev, got)
}

func TestBinCodec(t *testing.T) {
	codec := timebox.BinEvent

	ev := codecEvent()
	data, err := codec.Encode(ev)
	assert.NoError(t, err)

	got, err := codec.Decode(data)
	assert.NoError(t, err)
	assert.Equal(t, ev, got)
}

func TestBinCodecCorrupt(t *testing.T) {
	_, err := timebox.BinEvent.Decode([]byte{0x00, 0x01})
	assert.True(t, errors.Is(err, bin.ErrCorruptState))
}

func TestStreamEncode(t *testing.T) {
	codec := timebox.BinEvent
	ev1 := codecEvent()
	ev2 := codecEvent()

	buf, err := codec.Append(nil, ev1)
	assert.NoError(t, err)
	buf, err = codec.Append(buf, ev2)
	assert.NoError(t, err)

	assert.True(t, len(buf) > 0)
}

func TestStreamDecode(t *testing.T) {
	codec := timebox.BinEvent
	ev := codecEvent()

	buf, err := codec.Append(nil, ev)
	assert.NoError(t, err)
	buf, err = codec.Append(buf, ev)
	assert.NoError(t, err)

	ev1, rest, err := codec.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, ev, ev1)

	ev2, rest, err := codec.Read(rest)
	assert.NoError(t, err)
	assert.Equal(t, ev, ev2)
	assert.Len(t, rest, 0)
}

func TestJSONStreamEncode(t *testing.T) {
	codec := timebox.JSONEvent
	ev1 := codecEvent()
	ev2 := codecEvent()

	buf, err := codec.Append(nil, ev1)
	assert.NoError(t, err)
	buf, err = codec.Append(buf, ev2)
	assert.NoError(t, err)

	assert.True(t, len(buf) > 0)
}

func TestJSONAppendDecode(t *testing.T) {
	codec := timebox.JSONEvent
	ev := codecEvent()

	buf, err := codec.Append(nil, ev)
	assert.NoError(t, err)
	assert.True(t, len(buf) > 0)

	ev1, _, err := codec.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, ev, ev1)
}

func TestEncodeJSONEvents(t *testing.T) {
	ev1 := codecEvent()
	ev2 := codecEvent()
	evs := []*timebox.Event{ev1, ev2}

	data, err := timebox.EncodeJSONEvents(evs)
	assert.NoError(t, err)
	assert.Len(t, data, 2)
	assert.True(t, len(data[0]) > 0)
	assert.True(t, len(data[1]) > 0)
}

func TestEncodeJSONEventsEmpty(t *testing.T) {
	data, err := timebox.EncodeJSONEvents([]*timebox.Event{})
	assert.NoError(t, err)
	assert.Len(t, data, 0)
}

func TestDecodeJSONEvents(t *testing.T) {
	ev := codecEvent()
	encoded, err := timebox.JSONEvent.Encode(ev)
	assert.NoError(t, err)

	decoded, err := timebox.DecodeJSONEvents([][]byte{encoded, encoded})
	assert.NoError(t, err)
	assert.Len(t, decoded, 2)
	assert.Equal(t, ev, decoded[0])
	assert.Equal(t, ev, decoded[1])
}

func TestDecodeJSONEventsEmpty(t *testing.T) {
	decoded, err := timebox.DecodeJSONEvents([][]byte{})
	assert.NoError(t, err)
	assert.Len(t, decoded, 0)
}

func TestEncodeBinEvents(t *testing.T) {
	ev1 := codecEvent()
	ev2 := codecEvent()
	evs := []*timebox.Event{ev1, ev2}

	data, err := timebox.EncodeBinEvents(evs)
	assert.NoError(t, err)
	assert.Len(t, data, 2)
	assert.True(t, len(data[0]) > 0)
	assert.True(t, len(data[1]) > 0)
}

func TestDecodeBinEvents(t *testing.T) {
	ev := codecEvent()
	encoded, err := timebox.BinEvent.Encode(ev)
	assert.NoError(t, err)

	decoded, err := timebox.DecodeBinEvents([][]byte{encoded, encoded})
	assert.NoError(t, err)
	assert.Len(t, decoded, 2)
	assert.Equal(t, ev, decoded[0])
	assert.Equal(t, ev, decoded[1])
}

func TestAppendBinEvents(t *testing.T) {
	ev1 := codecEvent()
	ev2 := codecEvent()

	buf, err := timebox.BinEvent.AppendAll(nil, []*timebox.Event{ev1, ev2})
	assert.NoError(t, err)
	assert.NotEmpty(t, buf)

	n, rest, err := bin.ReadUint32(buf)
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), n)
	assert.NotEmpty(t, rest)
}

func TestReadBinEvents(t *testing.T) {
	ev := codecEvent()

	buf, err := timebox.BinEvent.AppendAll(nil, []*timebox.Event{ev, ev})
	assert.NoError(t, err)

	evs, rest, err := timebox.BinEvent.ReadAll(buf)
	assert.NoError(t, err)
	assert.Len(t, evs, 2)
	assert.Equal(t, ev, evs[0])
	assert.Equal(t, ev, evs[1])
	assert.Len(t, rest, 0)
}

func TestEncodeJSONEventsError(t *testing.T) {
	ev := &timebox.Event{
		Timestamp:   time.Unix(123, 456).UTC(),
		Sequence:    7,
		Type:        "event.test",
		AggregateID: timebox.NewAggregateID("flow", "1"),
		Data:        json.RawMessage{0xFF, 0xFE}, // invalid UTF-8
	}

	_, err := timebox.EncodeJSONEvents([]*timebox.Event{ev})
	assert.Error(t, err)
}

func TestDecodeJSONEventsError(t *testing.T) {
	_, err := timebox.DecodeJSONEvents([][]byte{[]byte("invalid json")})
	assert.Error(t, err)
}

func TestDecodeBinEventsError(t *testing.T) {
	_, err := timebox.DecodeBinEvents([][]byte{{0x00, 0x01}})
	assert.Error(t, err)
}

func TestReadBinEventsError(t *testing.T) {
	_, _, err := timebox.BinEvent.ReadAll([]byte{0x00, 0x01})
	assert.Error(t, err)
}

func codecEvent() *timebox.Event {
	return &timebox.Event{
		Timestamp:   time.Unix(123, 456).UTC(),
		Sequence:    7,
		Type:        "event.test",
		AggregateID: timebox.NewAggregateID("flow", "1"),
		Data:        json.RawMessage(`{"value":1}`),
	}
}

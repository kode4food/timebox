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

func TestEncodeJSONEvents(t *testing.T) {
	evs := []*timebox.Event{codecEvent(), codecEvent()}

	data, err := timebox.EncodeJSONEvents(evs)
	assert.NoError(t, err)
	assert.Len(t, data, 2)
}

func TestDecodeJSONEvents(t *testing.T) {
	ev := codecEvent()
	data, err := timebox.JSONEvent.Encode(ev)
	assert.NoError(t, err)

	evs, err := timebox.DecodeJSONEvents([]string{data, data})
	assert.NoError(t, err)
	assert.Len(t, evs, 2)
	assert.Equal(t, ev, evs[0])
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

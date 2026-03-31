package binary_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox/internal/binary"
)

func TestAppendReadUint32(t *testing.T) {
	buf := binary.AppendUint32(nil, 0xDEADBEEF)
	v, rest, err := binary.ReadUint32(buf)
	assert.NoError(t, err)
	assert.Equal(t, uint32(0xDEADBEEF), v)
	assert.Empty(t, rest)
}

func TestAppendReadUint64(t *testing.T) {
	buf := binary.AppendUint64(nil, 0xDEADBEEF01234567)
	v, rest, err := binary.ReadUint64(buf)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0xDEADBEEF01234567), v)
	assert.Empty(t, rest)
}

func TestAppendReadByte(t *testing.T) {
	buf := binary.AppendByte(nil, 0xAB)
	v, rest, err := binary.ReadByte(buf)
	assert.NoError(t, err)
	assert.Equal(t, byte(0xAB), v)
	assert.Empty(t, rest)
}

func TestAppendReadBool(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		buf := binary.AppendBool(nil, true)
		v, rest, err := binary.ReadBool(buf)
		assert.NoError(t, err)
		assert.True(t, v)
		assert.Empty(t, rest)
	})

	t.Run("false", func(t *testing.T) {
		buf := binary.AppendBool(nil, false)
		v, rest, err := binary.ReadBool(buf)
		assert.NoError(t, err)
		assert.False(t, v)
		assert.Empty(t, rest)
	})
}

func TestAppendReadInt64(t *testing.T) {
	buf := binary.AppendInt64(nil, -9876543210)
	v, rest, err := binary.ReadInt64(buf)
	assert.NoError(t, err)
	assert.Equal(t, int64(-9876543210), v)
	assert.Empty(t, rest)
}

func TestAppendReadString(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		buf := binary.AppendString(nil, "hello world")
		v, rest, err := binary.ReadString(buf)
		assert.NoError(t, err)
		assert.Equal(t, "hello world", v)
		assert.Empty(t, rest)
	})

	t.Run("empty", func(t *testing.T) {
		buf := binary.AppendString(nil, "")
		v, rest, err := binary.ReadString(buf)
		assert.NoError(t, err)
		assert.Equal(t, "", v)
		assert.Empty(t, rest)
	})
}

func TestAppendReadBytes(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		buf := binary.AppendBytes(nil, []byte{1, 2, 3, 4})
		v, rest, err := binary.ReadBytes(buf)
		assert.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3, 4}, v)
		assert.Empty(t, rest)
	})

	t.Run("nil", func(t *testing.T) {
		buf := binary.AppendBytes(nil, nil)
		v, rest, err := binary.ReadBytes(buf)
		assert.NoError(t, err)
		assert.Empty(t, v)
		assert.Empty(t, rest)
	})
}

func TestAppendReadOptString(t *testing.T) {
	t.Run("present", func(t *testing.T) {
		s := "active"
		buf := binary.AppendOptString(nil, &s)
		v, rest, err := binary.ReadOptString(buf)
		assert.NoError(t, err)
		assert.Equal(t, &s, v)
		assert.Empty(t, rest)
	})

	t.Run("nil", func(t *testing.T) {
		buf := binary.AppendOptString(nil, nil)
		v, rest, err := binary.ReadOptString(buf)
		assert.NoError(t, err)
		assert.Nil(t, v)
		assert.Empty(t, rest)
	})
}

func TestSequential(t *testing.T) {
	s := "label"
	buf := binary.AppendInt64(nil, -1)
	buf = binary.AppendString(buf, "event")
	buf = binary.AppendOptString(buf, &s)
	buf = binary.AppendOptString(buf, nil)
	buf = binary.AppendBytes(buf, []byte{0xFF})

	data := buf
	var err error

	var i int64
	i, data, err = binary.ReadInt64(data)
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), i)

	var str string
	str, data, err = binary.ReadString(data)
	assert.NoError(t, err)
	assert.Equal(t, "event", str)

	var opt *string
	opt, data, err = binary.ReadOptString(data)
	assert.NoError(t, err)
	assert.Equal(t, &s, opt)

	opt, data, err = binary.ReadOptString(data)
	assert.NoError(t, err)
	assert.Nil(t, opt)

	var b []byte
	b, data, err = binary.ReadBytes(data)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0xFF}, b)
	assert.Empty(t, data)
}

func TestReadErrors(t *testing.T) {
	t.Run("uint32 truncated", func(t *testing.T) {
		_, _, err := binary.ReadUint32([]byte{0x00, 0x00})
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("int64 truncated", func(t *testing.T) {
		_, _, err := binary.ReadInt64([]byte{0x00, 0x00, 0x00})
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("uint64 truncated", func(t *testing.T) {
		_, _, err := binary.ReadUint64([]byte{0x00, 0x00, 0x00})
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("string length truncated", func(t *testing.T) {
		_, _, err := binary.ReadString([]byte{0x00})
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("string data truncated", func(t *testing.T) {
		_, _, err := binary.ReadString([]byte{0x00, 0x00, 0x00, 0x05, 'a', 'b'})
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("bytes data truncated", func(t *testing.T) {
		_, _, err := binary.ReadBytes([]byte{0x00, 0x00, 0x00, 0x03, 0x01})
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("opt string empty", func(t *testing.T) {
		_, _, err := binary.ReadOptString([]byte{})
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("byte empty", func(t *testing.T) {
		_, _, err := binary.ReadByte(nil)
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})

	t.Run("bool empty", func(t *testing.T) {
		_, _, err := binary.ReadBool(nil)
		assert.True(t, errors.Is(err, binary.ErrCorruptState))
	})
}

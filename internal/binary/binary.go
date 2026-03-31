package binary

import (
	"encoding/binary"
	"errors"
)

var (
	// ErrCorruptState indicates stream failed encoding invariants
	ErrCorruptState = errors.New("corrupt encoded state")
)

var (
	AppendUint32 = binary.BigEndian.AppendUint32
	AppendUint64 = binary.BigEndian.AppendUint64
)

func AppendByte(buf []byte, v byte) []byte {
	return append(buf, v)
}

func AppendBool(buf []byte, v bool) []byte {
	if v {
		return append(buf, 1)
	}
	return append(buf, 0)
}

func AppendInt64(buf []byte, v int64) []byte {
	return AppendUint64(buf, uint64(v))
}

func AppendString(buf []byte, s string) []byte {
	buf = AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

func AppendBytes(buf []byte, data []byte) []byte {
	buf = AppendUint32(buf, uint32(len(data)))
	return append(buf, data...)
}

func AppendOptString(buf []byte, s *string) []byte {
	if s == nil {
		return append(buf, 0)
	}
	buf = append(buf, 1)
	return AppendString(buf, *s)
}

func ReadUint32(data []byte) (uint32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, ErrCorruptState
	}
	return binary.BigEndian.Uint32(data), data[4:], nil
}

func ReadUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, ErrCorruptState
	}
	return binary.BigEndian.Uint64(data), data[8:], nil
}

func ReadByte(data []byte) (byte, []byte, error) {
	if len(data) == 0 {
		return 0, nil, ErrCorruptState
	}
	return data[0], data[1:], nil
}

func ReadBool(data []byte) (bool, []byte, error) {
	v, data, err := ReadByte(data)
	if err != nil {
		return false, nil, err
	}
	return v != 0, data, nil
}

func ReadInt64(data []byte) (int64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, ErrCorruptState
	}
	return int64(binary.BigEndian.Uint64(data)), data[8:], nil
}

func ReadString(data []byte) (string, []byte, error) {
	n, data, err := ReadUint32(data)
	if err != nil {
		return "", nil, err
	}
	if len(data) < int(n) {
		return "", nil, ErrCorruptState
	}
	return string(data[:n]), data[n:], nil
}

func ReadBytes(data []byte) ([]byte, []byte, error) {
	n, data, err := ReadUint32(data)
	if err != nil {
		return nil, nil, err
	}
	if len(data) < int(n) {
		return nil, nil, ErrCorruptState
	}
	return data[:n], data[n:], nil
}

func ReadOptString(data []byte) (*string, []byte, error) {
	if len(data) == 0 {
		return nil, nil, ErrCorruptState
	}
	flag, data := data[0], data[1:]
	if flag == 0 {
		return nil, data, nil
	}
	s, data, err := ReadString(data)
	if err != nil {
		return nil, nil, err
	}
	return &s, data, nil
}

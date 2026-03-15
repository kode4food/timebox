package raft

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const (
	forwardRespOK byte = iota
	forwardRespNotLeader
	forwardRespTimeout
	forwardRespCanceled
	forwardRespInternal
)

func writeForwardRequest(
	w *bufio.Writer, timeout time.Duration, data []byte,
) error {
	if err := binary.Write(
		w, binary.BigEndian, timeout.Milliseconds(),
	); err != nil {
		return err
	}
	if err := writeForwardBytes(w, data); err != nil {
		return err
	}
	return w.Flush()
}

func readForwardRequest(r *bufio.Reader) (time.Duration, []byte, error) {
	var millis int64
	if err := binary.Read(r, binary.BigEndian, &millis); err != nil {
		return 0, nil, err
	}

	data, err := readForwardBytes(r)
	if err != nil {
		return 0, nil, err
	}
	return time.Duration(millis) * time.Millisecond, data, nil
}

func writeForwardResponse(w *bufio.Writer, res *applyResult, err error) error {
	code, msg := forwardResponseCode(err)
	data := []byte(nil)

	if code == forwardRespOK {
		enc, encErr := json.Marshal(normalizeApplyResult(res))
		if encErr != nil {
			code = forwardRespInternal
			msg = encErr.Error()
		} else {
			data = enc
		}
	}

	if err := w.WriteByte(code); err != nil {
		return err
	}
	if err := writeForwardBytes(w, []byte(msg)); err != nil {
		return err
	}
	if err := writeForwardBytes(w, data); err != nil {
		return err
	}
	return w.Flush()
}

func readForwardResponse(r *bufio.Reader) (*applyResult, error) {
	code, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	msg, err := readForwardBytes(r)
	if err != nil {
		return nil, err
	}
	data, err := readForwardBytes(r)
	if err != nil {
		return nil, err
	}

	if code != forwardRespOK {
		return nil, forwardResponseError(code, string(msg))
	}
	if len(data) == 0 {
		return &applyResult{}, nil
	}

	var res applyResult
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, err
	}
	if err := res.Err(); err != nil {
		return nil, err
	}
	return normalizeApplyResult(&res), nil
}

func writeForwardBytes(w *bufio.Writer, data []byte) error {
	size := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func readForwardBytes(r *bufio.Reader) ([]byte, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}

	data := make([]byte, size)
	_, err := io.ReadFull(r, data)
	return data, err
}

func forwardResponseCode(err error) (byte, string) {
	switch {
	case err == nil:
		return forwardRespOK, ""
	case errors.Is(err, ErrNotLeader):
		return forwardRespNotLeader, err.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return forwardRespTimeout, err.Error()
	case errors.Is(err, context.Canceled):
		return forwardRespCanceled, err.Error()
	default:
		return forwardRespInternal, err.Error()
	}
}

func forwardResponseError(code byte, msg string) error {
	switch code {
	case forwardRespNotLeader:
		return ErrNotLeader
	case forwardRespTimeout:
		return context.DeadlineExceeded
	case forwardRespCanceled:
		return context.Canceled
	default:
		if msg != "" {
			return errors.New(msg)
		}
		return errors.New("forwarded raft command failed")
	}
}

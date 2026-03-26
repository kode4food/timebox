package raft

import (
	"encoding/binary"
	"errors"
	"strconv"

	"github.com/kode4food/timebox"
	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	SnapshotCommand struct {
		ID       timebox.AggregateID
		Data     []byte
		Sequence int64
	}

	AggregateMeta struct {
		CurrentSequence  int64
		BaseSequence     int64
		SnapshotSequence int64
		Status           string
		StatusAt         int64
		Labels           map[string]string
	}

	ApplyResult struct {
		Append *timebox.AppendRequest
		Error  error
	}

	Command []byte
)

const (
	CmdTypeAppend   = 0
	CmdTypeSnapshot = 1

	cmdHeaderSize = 9 // 1 type byte + 8 proposalID bytes
)

var (
	// ErrUnexpectedApplyResult indicates the FSM returned an unexpected result
	ErrUnexpectedApplyResult = errors.New("unexpected raft apply result")

	// ErrCorruptState indicates local durable state failed local invariants
	ErrCorruptState = bin.ErrCorruptState

	// ErrCommandTypeUnknown indicates the FSM received an unknown command type
	ErrCommandTypeUnknown = errors.New("unknown command type")
)

func MakeAppendCommand(
	proposalID uint64, req *timebox.AppendRequest,
) (Command, error) {
	c := make(Command, 0, cmdHeaderSize+128)
	c = append(c, CmdTypeAppend)
	c = binary.BigEndian.AppendUint64(c, proposalID)
	c = appendAggregateID(c, req.ID)
	c = bin.AppendInt64(c, req.ExpectedSequence)
	c = bin.AppendOptString(c, req.Status)
	c = bin.AppendString(c, req.StatusAt)
	c = appendStrMap(c, req.Labels)
	return timebox.BinEvent.AppendAll(c, req.Events)
}

func MakeSnapshotCommand(proposalID uint64, sc *SnapshotCommand) Command {
	c := make(Command, 0, cmdHeaderSize+64)
	c = append(c, CmdTypeSnapshot)
	c = binary.BigEndian.AppendUint64(c, proposalID)
	c = appendAggregateID(c, sc.ID)
	c = bin.AppendInt64(c, sc.Sequence)
	c = bin.AppendBytes(c, sc.Data)
	return c
}

func (c Command) Type() int {
	if len(c) == 0 {
		return -1
	}
	return int(c[0])
}

func (c Command) ProposalID() (uint64, error) {
	if len(c) < cmdHeaderSize {
		return 0, ErrCorruptState
	}
	return binary.BigEndian.Uint64(c[1:9]), nil
}

func (c Command) AppendRequest() (*timebox.AppendRequest, error) {
	if len(c) < cmdHeaderSize {
		return nil, ErrCorruptState
	}
	return decodeAppendRequest(c[cmdHeaderSize:])
}

func (c Command) SnapshotRequest() (*SnapshotCommand, error) {
	if len(c) < cmdHeaderSize {
		return nil, ErrCorruptState
	}
	return decodeSnapshotCommand(c[cmdHeaderSize:])
}

func decodeAppendRequest(data []byte) (*timebox.AppendRequest, error) {
	id, data, err := readAggregateID(data)
	if err != nil {
		return nil, err
	}
	expectedSeq, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	status, data, err := bin.ReadOptString(data)
	if err != nil {
		return nil, err
	}
	statusAt, data, err := bin.ReadString(data)
	if err != nil {
		return nil, err
	}
	labels, data, err := readStrMap(data)
	if err != nil {
		return nil, err
	}
	events, _, err := timebox.BinEvent.ReadAll(data)
	if err != nil {
		return nil, err
	}
	return &timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: expectedSeq,
		Status:           status,
		StatusAt:         statusAt,
		Labels:           labels,
		Events:           events,
	}, nil
}

func decodeSnapshotCommand(data []byte) (*SnapshotCommand, error) {
	id, data, err := readAggregateID(data)
	if err != nil {
		return nil, err
	}
	seq, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	payload, _, err := bin.ReadBytes(data)
	if err != nil {
		return nil, err
	}
	return &SnapshotCommand{ID: id, Sequence: seq, Data: payload}, nil
}

func appendAggregateID(buf []byte, id timebox.AggregateID) []byte {
	buf = bin.AppendUint32(buf, uint32(len(id)))
	for _, part := range id {
		buf = bin.AppendString(buf, string(part))
	}
	return buf
}

func appendStrMap(buf []byte, m map[string]string) []byte {
	buf = bin.AppendUint32(buf, uint32(len(m)))
	for k, v := range m {
		buf = bin.AppendString(buf, k)
		buf = bin.AppendString(buf, v)
	}
	return buf
}

func readAggregateID(data []byte) (timebox.AggregateID, []byte, error) {
	n, data, err := bin.ReadUint32(data)
	if err != nil {
		return nil, nil, err
	}
	id := make(timebox.AggregateID, n)
	for i := range id {
		var s string
		s, data, err = bin.ReadString(data)
		if err != nil {
			return nil, nil, err
		}
		id[i] = timebox.ID(s)
	}
	return id, data, nil
}

func readStrMap(data []byte) (map[string]string, []byte, error) {
	n, data, err := bin.ReadUint32(data)
	if err != nil {
		return nil, nil, err
	}
	m := make(map[string]string, n)
	for range n {
		var k, v string
		k, data, err = bin.ReadString(data)
		if err != nil {
			return nil, nil, err
		}
		v, data, err = bin.ReadString(data)
		if err != nil {
			return nil, nil, err
		}
		m[k] = v
	}
	return m, data, nil
}

func marshalMeta(meta *AggregateMeta) []byte {
	buf := make([]byte, 0, 64)
	buf = bin.AppendInt64(buf, meta.CurrentSequence)
	buf = bin.AppendInt64(buf, meta.BaseSequence)
	buf = bin.AppendInt64(buf, meta.SnapshotSequence)
	buf = bin.AppendString(buf, meta.Status)
	buf = bin.AppendInt64(buf, meta.StatusAt)
	buf = appendStrMap(buf, meta.Labels)
	return buf
}

func unmarshalMeta(data []byte) (*AggregateMeta, error) {
	cur, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	base, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	snap, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	status, data, err := bin.ReadString(data)
	if err != nil {
		return nil, err
	}
	statusAt, data, err := bin.ReadInt64(data)
	if err != nil {
		return nil, err
	}
	labels, _, err := readStrMap(data)
	if err != nil {
		return nil, err
	}
	return &AggregateMeta{
		CurrentSequence:  cur,
		BaseSequence:     base,
		SnapshotSequence: snap,
		Status:           status,
		StatusAt:         statusAt,
		Labels:           labels,
	}, nil
}

func parseStatusAt(value string) int64 {
	ts, _ := strconv.ParseInt(value, 10, 64)
	return ts
}

func decodeOptionalInt64(value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}
	v, _, err := bin.ReadInt64(value)
	return v, err
}

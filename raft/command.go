package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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
		CurrentSequence  int64             `json:"cur"`
		BaseSequence     int64             `json:"base,omitempty"`
		SnapshotSequence int64             `json:"snap,omitempty"`
		Status           string            `json:"status,omitempty"`
		StatusAt         int64             `json:"statusAt,omitempty"`
		Labels           map[string]string `json:"labels,omitempty"`
	}

	ApplyResult struct {
		Conflict *timebox.AppendResult `json:"conflict,omitempty"`
		Status   string                `json:"status,omitempty"`
		Code     string                `json:"code,omitempty"`
		Message  string                `json:"message,omitempty"`
	}

	Command []byte
)

const (
	CmdTypeCompact  = 0
	CmdTypeAppend   = 1
	CmdTypeSnapshot = 2

	cmdHeaderSize  = 9  // 1 type byte + 8 proposalID bytes
	cmdCompactSize = 17 // header + 8 index bytes

	applyCodeInternal = "internal"
)

var (
	// ErrUnexpectedApplyResult indicates the FSM returned an unexpected result
	ErrUnexpectedApplyResult = errors.New("unexpected raft apply result")

	// ErrCorruptState indicates local durable state failed local invariants
	ErrCorruptState = bin.ErrCorruptState

	// ErrCommandTypeUnknown indicates the FSM received an unknown command type
	ErrCommandTypeUnknown = errors.New("unknown command type")
)

func MakeCompactCommand(proposalID, index uint64) Command {
	c := make(Command, cmdCompactSize)
	c[0] = CmdTypeCompact
	binary.BigEndian.PutUint64(c[1:9], proposalID)
	binary.BigEndian.PutUint64(c[9:17], index)
	return c
}

func MakeAppendCommand(proposalID uint64, req *timebox.AppendRequest) Command {
	c := make(Command, 0, cmdHeaderSize+128)
	c = append(c, CmdTypeAppend)
	c = binary.BigEndian.AppendUint64(c, proposalID)
	c = appendAggregateID(c, req.ID)
	c = bin.AppendInt64(c, req.ExpectedSequence)
	c = bin.AppendOptString(c, req.Status)
	c = bin.AppendString(c, req.StatusAt)
	c = appendStrMap(c, req.Labels)
	c = appendStrSlice(c, req.Events)
	return c
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

func (c Command) CompactIndex() uint64 {
	if len(c) < cmdCompactSize {
		return 0
	}
	return binary.BigEndian.Uint64(c[9:17])
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
	events, _, err := readStrSlice(data)
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

func appendStrSlice(buf []byte, ss []string) []byte {
	buf = bin.AppendUint32(buf, uint32(len(ss)))
	for _, s := range ss {
		buf = bin.AppendString(buf, s)
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

func readStrSlice(data []byte) ([]string, []byte, error) {
	n, data, err := bin.ReadUint32(data)
	if err != nil {
		return nil, nil, err
	}
	ss := make([]string, n)
	for i := range ss {
		ss[i], data, err = bin.ReadString(data)
		if err != nil {
			return nil, nil, err
		}
	}
	return ss, data, nil
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

func (r *ApplyResult) Err() error {
	if r.Code == "" {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrUnexpectedApplyResult, r.Message)
}

func marshalMeta(meta *AggregateMeta) ([]byte, error) {
	return json.Marshal(meta)
}

func unmarshalMeta(data []byte) (*AggregateMeta, error) {
	var meta AggregateMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	return &meta, nil
}

func parseStatusAt(value string) int64 {
	ts, _ := strconv.ParseInt(value, 10, 64)
	return ts
}

func encodeInt64(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

func decodeInt64(value []byte) (int64, error) {
	switch len(value) {
	case 0:
		return 0, nil
	case 8:
		return int64(binary.BigEndian.Uint64(value)), nil
	default:
		return 0, ErrCorruptState
	}
}

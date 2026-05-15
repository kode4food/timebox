package raft

import (
	"errors"
	"time"

	"github.com/kode4food/timebox"
	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	// SnapshotCommand carries one Timebox snapshot mutation through Raft
	SnapshotCommand struct {
		ID         timebox.AggregateID
		Data       []byte
		Sequence   int64
		TrimEvents bool
	}

	// ArchiveCommand carries one Timebox archive mutation through Raft
	ArchiveCommand struct {
		ID timebox.AggregateID
	}

	// ConsumeArchiveCommand carries one archive acknowledgement through Raft
	ConsumeArchiveCommand struct {
		StreamID string
	}

	// AggregateMeta stores the derived aggregate state needed for reads
	AggregateMeta struct {
		CurrentSequence  int64
		BaseSequence     int64
		SnapshotSequence int64
		Status           string
		StatusAt         int64
		Labels           map[string]string
	}

	// ApplyResult reports the local outcome of one applied Raft command
	ApplyResult struct {
		Append *timebox.AppendRequest
		Error  error
	}

	// Command is the encoded form of one replicated Timebox mutation
	Command []byte
)

const (
	CmdTypeAppend         = 0
	CmdTypeSnapshot       = 1
	CmdTypeArchive        = 2
	CmdTypeConsumeArchive = 3

	cmdHeaderSize = 9 // 1 type byte + 8 proposalID bytes
)

var (
	// ErrUnexpectedApplyResult indicates the FSM returned an unexpected result
	ErrUnexpectedApplyResult = errors.New("unexpected raft apply result")

	// ErrCommandTypeUnknown indicates the FSM received an unknown command type
	ErrCommandTypeUnknown = errors.New("unknown command type")
)

// MakeAppendCommand encodes one append mutation into a Raft command
func MakeAppendCommand(
	proposalID uint64, req *timebox.AppendRequest,
) (Command, error) {
	c := make(Command, 0, cmdHeaderSize+128)
	c = bin.AppendByte(c, CmdTypeAppend)
	c = bin.AppendUint64(c, proposalID)
	c = appendAggregateID(c, req.ID)
	c = bin.AppendInt64(c, req.ExpectedSequence)
	c = bin.AppendOptString(c, req.Status)
	c = bin.AppendInt64(c, req.StatusAt.UnixMilli())
	c = appendStrMap(c, req.Labels)
	return timebox.BinEvent.AppendAll(c, req.Events)
}

// MakeSnapshotCommand encodes one snapshot mutation into a Raft command
func MakeSnapshotCommand(proposalID uint64, sc *SnapshotCommand) Command {
	c := make(Command, 0, cmdHeaderSize+64)
	c = bin.AppendByte(c, CmdTypeSnapshot)
	c = bin.AppendUint64(c, proposalID)
	c = appendAggregateID(c, sc.ID)
	c = bin.AppendInt64(c, sc.Sequence)
	c = bin.AppendBool(c, sc.TrimEvents)
	c = bin.AppendBytes(c, sc.Data)
	return c
}

// MakeArchiveCommand encodes one archive mutation into a Raft command
func MakeArchiveCommand(proposalID uint64, ac *ArchiveCommand) Command {
	c := make(Command, 0, cmdHeaderSize+64)
	c = bin.AppendByte(c, CmdTypeArchive)
	c = bin.AppendUint64(c, proposalID)
	return appendAggregateID(c, ac.ID)
}

// MakeConsumeArchiveCommand encodes one archive ack into a Raft command
func MakeConsumeArchiveCommand(
	proposalID uint64, ac *ConsumeArchiveCommand,
) Command {
	c := make(Command, 0, cmdHeaderSize+32)
	c = bin.AppendByte(c, CmdTypeConsumeArchive)
	c = bin.AppendUint64(c, proposalID)
	return bin.AppendString(c, ac.StreamID)
}

// Type returns the encoded command type
func (c Command) Type() int {
	if len(c) == 0 {
		return -1
	}
	return int(c[0])
}

// ProposalID returns the encoded proposal ID
func (c Command) ProposalID() (uint64, error) {
	v, _, err := bin.ReadUint64(c[1:])
	return v, err
}

// AppendRequest decodes an append request from the command payload
func (c Command) AppendRequest() (*timebox.AppendRequest, error) {
	if len(c) < cmdHeaderSize {
		return nil, bin.ErrCorruptState
	}
	return decodeAppendRequest(c[cmdHeaderSize:])
}

// SnapshotRequest decodes a snapshot request from the command payload
func (c Command) SnapshotRequest() (*SnapshotCommand, error) {
	if len(c) < cmdHeaderSize {
		return nil, bin.ErrCorruptState
	}
	return decodeSnapshotCommand(c[cmdHeaderSize:])
}

// ArchiveRequest decodes an archive request from the command payload
func (c Command) ArchiveRequest() (*ArchiveCommand, error) {
	if len(c) < cmdHeaderSize {
		return nil, bin.ErrCorruptState
	}
	return decodeArchiveCommand(c[cmdHeaderSize:])
}

// ConsumeArchiveRequest decodes an archive ack from the command payload
func (c Command) ConsumeArchiveRequest() (*ConsumeArchiveCommand, error) {
	if len(c) < cmdHeaderSize {
		return nil, bin.ErrCorruptState
	}
	return decodeConsumeArchiveCommand(c[cmdHeaderSize:])
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
	statusAt, data, err := bin.ReadInt64(data)
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
		StatusAt:         time.UnixMilli(statusAt).UTC(),
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
	trimEvents, data, err := bin.ReadBool(data)
	if err != nil {
		return nil, err
	}
	payload, _, err := bin.ReadBytes(data)
	if err != nil {
		return nil, err
	}
	return &SnapshotCommand{
		ID:         id,
		Sequence:   seq,
		TrimEvents: trimEvents,
		Data:       payload,
	}, nil
}

func decodeArchiveCommand(data []byte) (*ArchiveCommand, error) {
	id, _, err := readAggregateID(data)
	if err != nil {
		return nil, err
	}
	return &ArchiveCommand{ID: id}, nil
}

func decodeConsumeArchiveCommand(data []byte) (*ConsumeArchiveCommand, error) {
	streamID, _, err := bin.ReadString(data)
	if err != nil {
		return nil, err
	}
	return &ConsumeArchiveCommand{StreamID: streamID}, nil
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

func decodeOptionalInt64(value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}
	v, _, err := bin.ReadInt64(value)
	return v, err
}

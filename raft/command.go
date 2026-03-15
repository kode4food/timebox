package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/kode4food/timebox"
)

type (
	byteReader interface {
		io.ByteReader
		io.Reader
	}

	commandType string

	command struct {
		ProposalID string
		Type       commandType
		Append     *appendCommand
		Snapshot   *snapshotCommand
		Compact    *compactCommand
	}

	appendCommand struct {
		Request timebox.AppendRequest
	}

	snapshotCommand struct {
		ID       timebox.AggregateID
		Data     []byte
		Sequence int64
	}

	compactCommand struct {
		Index uint64
	}

	aggregateMeta struct {
		CurrentSequence  int64
		BaseSequence     int64
		SnapshotSequence int64
		Status           string
		StatusAt         int64
		Labels           map[string]string
	}

	applyResult struct {
		Conflict *timebox.AppendResult `json:"conflict,omitempty"`
		Status   string                `json:"status,omitempty"`
		Code     string                `json:"code,omitempty"`
		Message  string                `json:"message,omitempty"`
	}
)

const (
	commandAppend   commandType = "append"
	commandSnapshot commandType = "snapshot"
	commandCompact  commandType = "compact"

	applyCodeInvalidCommand = "invalid-command"
	applyCodeCorruptState   = "corrupt-state"
	applyCodeInternal       = "internal"

	commandCodecVersion byte = 1
	metaCodecVersion    byte = 1

	commandKindAppend byte = iota + 1
	commandKindSnapshot
	commandKindCompact
)

var (
	// ErrUnexpectedApplyResult indicates the FSM returned an unexpected result
	ErrUnexpectedApplyResult = errors.New("unexpected raft apply result")

	// ErrCorruptState indicates local durable state failed local invariants
	ErrCorruptState = errors.New("corrupt raft persistence state")

	// ErrAppendCommandMissing indicates an append command lacks payload
	ErrAppendCommandMissing = errors.New("append payload missing")

	// ErrSnapshotCommandMissing indicates a snapshot command lacks payload
	ErrSnapshotCommandMissing = errors.New("snapshot payload missing")

	// ErrCompactCommandMissing indicates a compact command lacks payload
	ErrCompactCommandMissing = errors.New("compact payload missing")

	// ErrCommandTypeUnknown indicates the FSM received an unknown command type
	ErrCommandTypeUnknown = errors.New("unknown command type")
)

func (r *applyResult) Err() error {
	if r.Code == "" {
		return nil
	}
	switch r.Code {
	case applyCodeInvalidCommand:
		return fmt.Errorf("%w: %s", ErrUnexpectedApplyResult, r.Message)
	case applyCodeCorruptState:
		return fmt.Errorf("%w: %s", ErrCorruptState, r.Message)
	default:
		return fmt.Errorf("%w: %s", ErrUnexpectedApplyResult, r.Message)
	}
}

func encodeCommand(cmd command) ([]byte, error) {
	var buf bytes.Buffer

	if err := buf.WriteByte(commandCodecVersion); err != nil {
		return nil, err
	}
	if err := writeCommandType(&buf, cmd.Type); err != nil {
		return nil, err
	}
	if err := writeString(&buf, cmd.ProposalID); err != nil {
		return nil, err
	}

	switch cmd.Type {
	case commandAppend:
		if cmd.Append == nil {
			return nil, ErrAppendCommandMissing
		}
		if err := writeAppendCommand(&buf, cmd.Append); err != nil {
			return nil, err
		}
	case commandSnapshot:
		if cmd.Snapshot == nil {
			return nil, ErrSnapshotCommandMissing
		}
		if err := writeSnapshotCommand(&buf, cmd.Snapshot); err != nil {
			return nil, err
		}
	case commandCompact:
		if cmd.Compact == nil {
			return nil, ErrCompactCommandMissing
		}
		if err := writeCompactCommand(&buf, cmd.Compact); err != nil {
			return nil, err
		}
	default:
		return nil, ErrCommandTypeUnknown
	}

	return buf.Bytes(), nil
}

func decodeCommand(data []byte) (*command, error) {
	if len(data) == 0 {
		return nil, ErrCommandTypeUnknown
	}
	if data[0] == '{' {
		return decodeLegacyCommand(data)
	}

	r := bytes.NewReader(data)
	version, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if version != commandCodecVersion {
		return nil, ErrCommandTypeUnknown
	}

	typ, err := readCommandType(r)
	if err != nil {
		return nil, err
	}
	proposalID, err := readString(r)
	if err != nil {
		return nil, err
	}

	cmd := &command{
		ProposalID: proposalID,
		Type:       typ,
	}

	switch typ {
	case commandAppend:
		appendCmd, err := readAppendCommand(r)
		if err != nil {
			return nil, err
		}
		cmd.Append = appendCmd
	case commandSnapshot:
		snapshotCmd, err := readSnapshotCommand(r)
		if err != nil {
			return nil, err
		}
		cmd.Snapshot = snapshotCmd
	case commandCompact:
		compactCmd, err := readCompactCommand(r)
		if err != nil {
			return nil, err
		}
		cmd.Compact = compactCmd
	default:
		return nil, ErrCommandTypeUnknown
	}

	return cmd, nil
}

func decodeProposalID(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	if data[0] == '{' {
		cmd, err := decodeLegacyCommand(data)
		if err != nil {
			return ""
		}
		return cmd.ProposalID
	}

	r := bytes.NewReader(data)
	version, err := r.ReadByte()
	if err != nil || version != commandCodecVersion {
		return ""
	}
	if _, err := readCommandType(r); err != nil {
		return ""
	}
	proposalID, err := readString(r)
	if err != nil {
		return ""
	}
	return proposalID
}

func peekCommandType(data []byte) commandType {
	if len(data) == 0 || data[0] == '{' {
		return ""
	}

	r := bytes.NewReader(data)
	version, err := r.ReadByte()
	if err != nil || version != commandCodecVersion {
		return ""
	}

	typ, err := readCommandType(r)
	if err != nil {
		return ""
	}
	return typ
}

func marshalMeta(meta *aggregateMeta) ([]byte, error) {
	if meta == nil {
		meta = &aggregateMeta{}
	}

	var buf bytes.Buffer
	if err := buf.WriteByte(metaCodecVersion); err != nil {
		return nil, err
	}
	if err := writeVarint(&buf, meta.CurrentSequence); err != nil {
		return nil, err
	}
	if err := writeVarint(&buf, meta.BaseSequence); err != nil {
		return nil, err
	}
	if err := writeVarint(&buf, meta.SnapshotSequence); err != nil {
		return nil, err
	}
	if err := writeString(&buf, meta.Status); err != nil {
		return nil, err
	}
	if err := writeVarint(&buf, meta.StatusAt); err != nil {
		return nil, err
	}
	if err := writeStringMap(&buf, meta.Labels); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshalMeta(data []byte) (*aggregateMeta, error) {
	if len(data) == 0 {
		return &aggregateMeta{Labels: map[string]string{}}, nil
	}
	if data[0] == '{' {
		return decodeLegacyMeta(data)
	}

	r := bytes.NewReader(data)
	version, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if version != metaCodecVersion {
		return nil, ErrCorruptState
	}

	currentSequence, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	baseSequence, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	snapshotSequence, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	status, err := readString(r)
	if err != nil {
		return nil, err
	}
	statusAt, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	labels, err := readStringMap(r)
	if err != nil {
		return nil, err
	}
	if labels == nil {
		labels = map[string]string{}
	}

	return &aggregateMeta{
		CurrentSequence:  currentSequence,
		BaseSequence:     baseSequence,
		SnapshotSequence: snapshotSequence,
		Status:           status,
		StatusAt:         statusAt,
		Labels:           labels,
	}, nil
}

func parseStatusAt(value string) int64 {
	if value == "" {
		return 0
	}
	ts, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return ts
}

func encodeApplyError(code string, err error) *applyResult {
	if err == nil {
		return nil
	}
	return &applyResult{
		Code:    code,
		Message: err.Error(),
	}
}

func normalizeApplyResult(res *applyResult) *applyResult {
	if res == nil {
		return &applyResult{}
	}
	return res
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
		return strconv.ParseInt(string(value), 10, 64)
	}
}

func decodeLegacyCommand(data []byte) (*command, error) {
	var cmd command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

func decodeLegacyMeta(data []byte) (*aggregateMeta, error) {
	var meta aggregateMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	return &meta, nil
}

func writeCommandType(w io.Writer, typ commandType) error {
	var kind byte

	switch typ {
	case commandAppend:
		kind = commandKindAppend
	case commandSnapshot:
		kind = commandKindSnapshot
	case commandCompact:
		kind = commandKindCompact
	default:
		return ErrCommandTypeUnknown
	}

	_, err := w.Write([]byte{kind})
	return err
}

func readCommandType(r io.ByteReader) (commandType, error) {
	kind, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	switch kind {
	case commandKindAppend:
		return commandAppend, nil
	case commandKindSnapshot:
		return commandSnapshot, nil
	case commandKindCompact:
		return commandCompact, nil
	default:
		return "", ErrCommandTypeUnknown
	}
}

func writeAppendCommand(w io.Writer, cmd *appendCommand) error {
	req := cmd.Request
	if err := writeAggregateID(w, req.ID); err != nil {
		return err
	}
	if err := writeVarint(w, req.ExpectedSequence); err != nil {
		return err
	}
	if err := writeOptionalString(w, req.Status); err != nil {
		return err
	}
	if err := writeString(w, req.StatusAt); err != nil {
		return err
	}
	if err := writeStringMap(w, req.Labels); err != nil {
		return err
	}
	return writeStringSlice(w, req.Events)
}

func readAppendCommand(r byteReader) (*appendCommand, error) {
	id, err := readAggregateID(r)
	if err != nil {
		return nil, err
	}
	expectedSequence, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	status, err := readOptionalString(r)
	if err != nil {
		return nil, err
	}
	statusAt, err := readString(r)
	if err != nil {
		return nil, err
	}
	labels, err := readStringMap(r)
	if err != nil {
		return nil, err
	}
	events, err := readStringSlice(r)
	if err != nil {
		return nil, err
	}

	return &appendCommand{
		Request: timebox.AppendRequest{
			ID:               id,
			ExpectedSequence: expectedSequence,
			Status:           status,
			StatusAt:         statusAt,
			Labels:           labels,
			Events:           events,
		},
	}, nil
}

func writeSnapshotCommand(w io.Writer, cmd *snapshotCommand) error {
	if err := writeAggregateID(w, cmd.ID); err != nil {
		return err
	}
	if err := writeBytes(w, cmd.Data); err != nil {
		return err
	}
	return writeVarint(w, cmd.Sequence)
}

func readSnapshotCommand(r byteReader) (*snapshotCommand, error) {
	id, err := readAggregateID(r)
	if err != nil {
		return nil, err
	}
	data, err := readBytes(r)
	if err != nil {
		return nil, err
	}
	sequence, err := readVarint(r)
	if err != nil {
		return nil, err
	}
	return &snapshotCommand{
		ID:       id,
		Data:     data,
		Sequence: sequence,
	}, nil
}

func writeCompactCommand(w io.Writer, cmd *compactCommand) error {
	return writeUvarint(w, cmd.Index)
}

func readCompactCommand(r byteReader) (*compactCommand, error) {
	index, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	return &compactCommand{Index: index}, nil
}

func writeAggregateID(w io.Writer, id timebox.AggregateID) error {
	if err := writeUvarint(w, uint64(len(id))); err != nil {
		return err
	}
	for _, part := range id {
		if err := writeString(w, string(part)); err != nil {
			return err
		}
	}
	return nil
}

func readAggregateID(r byteReader) (timebox.AggregateID, error) {
	count, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return timebox.AggregateID{}, nil
	}

	id := make(timebox.AggregateID, int(count))
	for i := range id {
		part, err := readString(r)
		if err != nil {
			return nil, err
		}
		id[i] = timebox.ID(part)
	}
	return id, nil
}

func writeStringSlice(w io.Writer, values []string) error {
	if err := writeUvarint(w, uint64(len(values))); err != nil {
		return err
	}
	for _, value := range values {
		if err := writeString(w, value); err != nil {
			return err
		}
	}
	return nil
}

func readStringSlice(r byteReader) ([]string, error) {
	count, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	values := make([]string, int(count))
	for i := range values {
		value, err := readString(r)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

func writeStringMap(w io.Writer, values map[string]string) error {
	if err := writeUvarint(w, uint64(len(values))); err != nil {
		return err
	}
	for key, value := range values {
		if err := writeString(w, key); err != nil {
			return err
		}
		if err := writeString(w, value); err != nil {
			return err
		}
	}
	return nil
}

func readStringMap(r byteReader) (map[string]string, error) {
	count, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}

	values := make(map[string]string, int(count))
	for range count {
		key, err := readString(r)
		if err != nil {
			return nil, err
		}
		value, err := readString(r)
		if err != nil {
			return nil, err
		}
		values[key] = value
	}
	return values, nil
}

func writeOptionalString(w io.Writer, value *string) error {
	if value == nil {
		_, err := w.Write([]byte{0})
		return err
	}
	if _, err := w.Write([]byte{1}); err != nil {
		return err
	}
	return writeString(w, *value)
}

func readOptionalString(r byteReader) (*string, error) {
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if flag == 0 {
		return nil, nil
	}
	value, err := readString(r)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func writeString(w io.Writer, value string) error {
	return writeBytes(w, []byte(value))
}

func readString(r byteReader) (string, error) {
	value, err := readBytes(r)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func writeBytes(w io.Writer, data []byte) error {
	if err := writeUvarint(w, uint64(len(data))); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func readBytes(r byteReader) ([]byte, error) {
	size, err := readUvarint(r)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}

	res := make([]byte, int(size))
	if _, err := io.ReadFull(r, res); err != nil {
		return nil, err
	}
	return res, nil
}

func writeUvarint(w io.Writer, value uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	_, err := w.Write(buf[:n])
	return err
}

func readUvarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func writeVarint(w io.Writer, value int64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], value)
	_, err := w.Write(buf[:n])
	return err
}

func readVarint(r io.ByteReader) (int64, error) {
	return binary.ReadVarint(r)
}

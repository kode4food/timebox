package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/kode4food/timebox"
)

type (
	commandType string

	command struct {
		ProposalID string           `json:"pid,omitempty"`
		Type       commandType      `json:"type"`
		Append     *appendCommand   `json:"append,omitempty"`
		Snapshot   *snapshotCommand `json:"snapshot,omitempty"`
		Compact    *compactCommand  `json:"compact,omitempty"`
	}

	appendCommand struct {
		Request timebox.AppendRequest `json:"request"`
	}

	snapshotCommand struct {
		ID       timebox.AggregateID `json:"id"`
		Data     []byte              `json:"data"`
		Sequence int64               `json:"seq"`
	}

	compactCommand struct {
		Index uint64 `json:"index"`
	}

	aggregateMeta struct {
		CurrentSequence  int64             `json:"cur"`
		BaseSequence     int64             `json:"base,omitempty"`
		SnapshotSequence int64             `json:"snap,omitempty"`
		Status           string            `json:"status,omitempty"`
		StatusAt         int64             `json:"statusAt,omitempty"`
		Labels           map[string]string `json:"labels,omitempty"`
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

	applyCodeInternal = "internal"
)

var (
	// ErrUnexpectedApplyResult indicates the FSM returned an unexpected result
	ErrUnexpectedApplyResult = errors.New("unexpected raft apply result")

	// ErrCorruptState indicates local durable state failed local invariants
	ErrCorruptState = errors.New("corrupt raft persistence state")

	// ErrCompactCommandMissing indicates a compact command lacks payload
	ErrCompactCommandMissing = errors.New("compact payload missing")

	// ErrCommandTypeUnknown indicates the FSM received an unknown command type
	ErrCommandTypeUnknown = errors.New("unknown command type")
)

func (r *applyResult) Err() error {
	if r.Code == "" {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrUnexpectedApplyResult, r.Message)
}

func encodeCommand(cmd command) ([]byte, error) {
	return json.Marshal(cmd)
}

func decodeCommand(data []byte) (*command, error) {
	var cmd command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

func marshalMeta(meta *aggregateMeta) ([]byte, error) {
	return json.Marshal(meta)
}

func unmarshalMeta(data []byte) (*aggregateMeta, error) {
	var meta aggregateMeta
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
		return 0, ErrCorruptState
	}
}

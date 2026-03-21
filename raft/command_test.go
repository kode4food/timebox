package raft_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/raft"
)

func TestCommandType(t *testing.T) {
	t.Run("empty returns -1", func(t *testing.T) {
		assert.Equal(t, -1, raft.Command(nil).Type())
	})

	t.Run("append", func(t *testing.T) {
		c, err := raft.MakeAppendCommand(1, &timebox.AppendRequest{
			ID: timebox.NewAggregateID("ns", "id"),
		})
		assert.NoError(t, err)
		assert.Equal(t, raft.CmdTypeAppend, c.Type())
	})

	t.Run("snapshot", func(t *testing.T) {
		c := raft.MakeSnapshotCommand(1, &raft.SnapshotCommand{
			ID: timebox.NewAggregateID("ns", "id"),
		})
		assert.Equal(t, raft.CmdTypeSnapshot, c.Type())
	})
}

func TestCommandProposalID(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		c := raft.MakeSnapshotCommand(42, &raft.SnapshotCommand{
			ID: timebox.NewAggregateID("ns", "id"),
		})
		pid, err := c.ProposalID()
		assert.NoError(t, err)
		assert.Equal(t, uint64(42), pid)
	})

	t.Run("too short", func(t *testing.T) {
		_, err := raft.Command([]byte{0x01}).ProposalID()
		assert.True(t, errors.Is(err, raft.ErrCorruptState))
	})
}

func TestCommandAppendRoundtrip(t *testing.T) {
	status := "active"
	id := timebox.NewAggregateID("ns", "id1")
	evs := testEvents()
	for i, ev := range evs {
		ev.AggregateID = append(timebox.AggregateID(nil), id...)
		ev.Sequence = 7 + int64(i)
	}
	req := &timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 7,
		Status:           &status,
		StatusAt:         time.Now().UTC().Format(time.RFC3339),
		Labels:           map[string]string{"env": "prod"},
		Events:           evs,
	}
	c, err := raft.MakeAppendCommand(99, req)
	assert.NoError(t, err)

	pid, err := c.ProposalID()
	assert.NoError(t, err)
	assert.Equal(t, uint64(99), pid)

	got, err := c.AppendRequest()
	assert.NoError(t, err)
	assert.Equal(t, req.ID, got.ID)
	assert.Equal(t, req.ExpectedSequence, got.ExpectedSequence)
	assert.Equal(t, req.Status, got.Status)
	assert.Equal(t, req.StatusAt, got.StatusAt)
	assert.Equal(t, req.Labels, got.Labels)
	assert.Equal(t, req.Events, got.Events)
}

func TestCommandSnapshotRoundtrip(t *testing.T) {
	sc := &raft.SnapshotCommand{
		ID:       timebox.NewAggregateID("ns", "id2"),
		Sequence: 3,
		Data:     []byte{0xDE, 0xAD, 0xBE, 0xEF},
	}
	c := raft.MakeSnapshotCommand(77, sc)

	pid, err := c.ProposalID()
	assert.NoError(t, err)
	assert.Equal(t, uint64(77), pid)

	got, err := c.SnapshotRequest()
	assert.NoError(t, err)
	assert.Equal(t, sc.ID, got.ID)
	assert.Equal(t, sc.Sequence, got.Sequence)
	assert.Equal(t, sc.Data, got.Data)
}

func TestCommandCorrupt(t *testing.T) {
	t.Run("append too short", func(t *testing.T) {
		_, err := raft.Command([]byte{raft.CmdTypeAppend}).AppendRequest()
		assert.True(t, errors.Is(err, raft.ErrCorruptState))
	})

	t.Run("append corrupt payload", func(t *testing.T) {
		c := make(raft.Command, 9+4) // header + truncated payload
		c[0] = raft.CmdTypeAppend
		_, err := c.AppendRequest()
		assert.True(t, errors.Is(err, raft.ErrCorruptState))
	})

	t.Run("snapshot too short", func(t *testing.T) {
		_, err := raft.Command([]byte{raft.CmdTypeSnapshot}).SnapshotRequest()
		assert.True(t, errors.Is(err, raft.ErrCorruptState))
	})

	t.Run("snapshot corrupt payload", func(t *testing.T) {
		c := make(raft.Command, 9+4) // header + truncated payload
		c[0] = raft.CmdTypeSnapshot
		_, err := c.SnapshotRequest()
		assert.True(t, errors.Is(err, raft.ErrCorruptState))
	})
}

func testEvents() []*timebox.Event {
	return []*timebox.Event{
		{
			Timestamp: time.Unix(1, 0).UTC(),
			Type:      "event.one",
			Data:      json.RawMessage(`1`),
		},
		{
			Timestamp: time.Unix(2, 0).UTC(),
			Type:      "event.two",
			Data:      json.RawMessage(`2`),
		},
	}
}

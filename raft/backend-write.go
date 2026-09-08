package raft

import (
	"context"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/check"
)

// Append proposes every append mutation through the local Raft node
func (b *Backend) Append(reqs ...timebox.AppendRequest) error {
	if err := check.Distinct(reqs); err != nil {
		return err
	}

	var events []*timebox.Event
	mutates := false
	for _, req := range reqs {
		if err := b.checkConflict(req.ID, req.ExpectedSequence); err != nil {
			return err
		}
		if check.Mutates(req) {
			mutates = true
		}
		events = append(events, req.Events...)
	}
	if !mutates {
		return nil
	}

	propID := b.newProposalID()
	cmd, err := MakeAppendCommand(propID, reqs)
	if err != nil {
		return err
	}
	res, err := b.applyWithTimeout(
		context.Background(), cmd, propID, events,
	)
	if err != nil {
		return err
	}
	return res.Error
}

// SaveSnapshot proposes one Timebox snapshot mutation through Raft
func (b *Backend) SaveSnapshot(req timebox.SnapshotRequest) error {
	propID := b.newProposalID()
	_, err := b.applyWithTimeout(
		context.Background(),
		MakeSnapshotCommand(propID, &SnapshotCommand{
			ID:         req.ID,
			Data:       req.Data,
			Sequence:   req.Sequence,
			TrimEvents: req.TrimEvents,
		}),
		propID,
		nil,
	)
	return err
}

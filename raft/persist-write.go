package raft

import (
	"context"

	"github.com/kode4food/timebox"
)

// Append proposes one append mutation through the local Raft node
func (p *Persistence) Append(req timebox.AppendRequest) error {
	if err := p.checkConflict(
		req.ID, req.ExpectedSequence,
	); err != nil {
		return err
	}
	propID := p.newProposalID()
	cmd, err := MakeAppendCommand(propID, &req)
	if err != nil {
		return err
	}
	res, err := p.applyWithTimeout(
		context.Background(),
		cmd,
		propID,
		req.Events,
	)
	if err != nil {
		return err
	}
	return res.Error
}

// SaveSnapshot proposes one Timebox snapshot mutation through Raft
func (p *Persistence) SaveSnapshot(req timebox.SnapshotRequest) error {
	propID := p.newProposalID()
	_, err := p.applyWithTimeout(
		context.Background(),
		MakeSnapshotCommand(propID, &SnapshotCommand{
			ID:         req.ID,
			Data:       req.Data,
			Sequence:   req.Sequence,
			TrimEvents: req.Config().TrimEvents,
		}),
		propID,
		nil,
	)
	return err
}

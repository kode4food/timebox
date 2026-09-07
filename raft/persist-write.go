package raft

import (
	"context"

	"github.com/kode4food/timebox"
)

// Append proposes every append mutation through the local Raft node
func (p *Persistence) Append(reqs ...timebox.AppendRequest) error {
	var events []*timebox.Event
	mutates := false
	for _, req := range reqs {
		if err := p.checkConflict(req.ID, req.ExpectedSequence); err != nil {
			return err
		}
		if len(req.Events) != 0 || req.Status != nil || len(req.Tags) != 0 {
			mutates = true
		}
		events = append(events, req.Events...)
	}
	if !mutates {
		return nil
	}

	propID := p.newProposalID()
	cmd, err := MakeAppendCommand(propID, reqs)
	if err != nil {
		return err
	}
	res, err := p.applyWithTimeout(
		context.Background(), cmd, propID, events,
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

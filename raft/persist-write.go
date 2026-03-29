package raft

import (
	"context"

	"github.com/kode4food/timebox"
)

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

func (p *Persistence) SaveSnapshot(
	id timebox.AggregateID, data []byte, sequence int64,
) error {
	propID := p.newProposalID()
	_, err := p.applyWithTimeout(
		context.Background(),
		MakeSnapshotCommand(propID, &SnapshotCommand{
			ID:       id,
			Data:     data,
			Sequence: sequence,
		}),
		propID,
		nil,
	)
	return err
}

func (p *Persistence) CanSaveSnapshot() bool {
	return len(p.peers) <= 1
}

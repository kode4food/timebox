package raft

import (
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kode4food/timebox"
)

func (p *Persistence) handleReady(rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := p.applySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if err := p.raftLog.Save(rd, p.compactBound()); err != nil {
		return err
	}
	if err := p.queueMessages(rd.Messages); err != nil {
		return err
	}
	if len(rd.CommittedEntries) != 0 {
		p.lastCommitAt = time.Now()
	}
	if err := p.applyCommittedEntries(rd.CommittedEntries); err != nil {
		return err
	}
	if p.State() == StateLeader {
		p.markReady()
	} else if len(rd.CommittedEntries) == 0 {
		p.markReadyFollower()
	}
	p.node.Advance()
	return nil
}

func (p *Persistence) applyCommittedEntries(ents []raftpb.Entry) error {
	return p.applyEntries(ents, p.applyConfChange)
}

func (p *Persistence) applyStartupEntries(ents []raftpb.Entry) error {
	return p.applyEntries(ents, func(ent raftpb.Entry) error {
		return p.markAppliedEntry(ent.Index)
	})
}

func (p *Persistence) applyEntries(
	ents []raftpb.Entry, confChange func(raftpb.Entry) error,
) error {
	if len(ents) == 0 {
		return nil
	}

	var batch []decodedEntry
	var propIDs []uint64

	flushAndReset := func() error {
		if err := p.flushBatch(batch, propIDs); err != nil {
			return err
		}
		batch = batch[:0]
		propIDs = propIDs[:0]
		return nil
	}

	for _, ent := range ents {
		switch ent.Type {
		case raftpb.EntryConfChange,
			raftpb.EntryConfChangeV2:
			if err := flushAndReset(); err != nil {
				return err
			}
			if err := confChange(ent); err != nil {
				return err
			}
		case raftpb.EntryNormal:
			if len(ent.Data) == 0 {
				if err := flushAndReset(); err != nil {
					return err
				}
				if err := p.markAppliedEntry(ent.Index); err != nil {
					return err
				}
				continue
			}
			cmd := Command(ent.Data)
			propID, err := cmd.ProposalID()
			if err != nil {
				return err
			}
			batch = append(batch, decodedEntry{
				index: ent.Index,
				cmd:   cmd,
			})
			propIDs = append(propIDs, propID)
		default:
			if err := flushAndReset(); err != nil {
				return err
			}
			if err := p.markAppliedEntry(ent.Index); err != nil {
				return err
			}
		}
	}
	return p.flushBatch(batch, propIDs)
}

func (p *Persistence) flushBatchNoPublish(
	batch []decodedEntry, propIDs []uint64,
) error {
	if len(batch) == 0 {
		return nil
	}
	results, err := p.fsm.applyEntries(batch)
	if err != nil {
		return err
	}
	for i := range batch {
		p.resolveProposal(propIDs[i], batch[i].cmd, results[i])
		p.notifyAppliedArchive(batch[i].cmd, results[i])
	}
	p.appliedIndex.Store(batch[len(batch)-1].index)
	return nil
}

func (p *Persistence) flushBatchPublish(
	batch []decodedEntry, propIDs []uint64,
) error {
	if len(batch) == 0 {
		return nil
	}
	results, err := p.fsm.applyEntries(batch)
	if err != nil {
		return err
	}
	var published []*timebox.Event
	for i := range batch {
		res := results[i]
		p.resolveProposal(propIDs[i], batch[i].cmd, res)
		p.notifyAppliedArchive(batch[i].cmd, res)
		if res.Error != nil {
			continue
		}
		if evs := p.proposalEvents(propIDs[i], batch[i].cmd); len(evs) > 0 {
			published = append(published, evs...)
		} else if res.Append != nil {
			published = append(published, res.Append.Events...)
		}
	}
	p.appliedIndex.Store(batch[len(batch)-1].index)
	if len(published) != 0 {
		p.publishQ.Put(published)
	}
	return nil
}

func (p *Persistence) notifyAppliedArchive(cmd Command, res *ApplyResult) {
	if res.Error != nil {
		return
	}
	switch cmd.Type() {
	case CmdTypeArchive, CmdTypeConsumeArchive:
		p.notifyArchive()
	}
}

func (p *Persistence) applyConfChange(ent raftpb.Entry) error {
	if len(ent.Data) == 0 {
		return p.markAppliedEntry(ent.Index)
	}
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(ent.Data); err != nil {
		return err
	}
	cs := p.node.ApplyConfChange(cc)
	if err := p.raftLog.SetConfState(*cs); err != nil {
		return err
	}
	return p.markAppliedEntry(ent.Index)
}

func (p *Persistence) markReady() {
	if p.Publisher != nil {
		p.flushBatch = p.flushBatchPublish
	}
	p.readyOnce.Do(func() {
		close(p.readyCh)
	})
}

func (p *Persistence) markReadyFollower() {
	if !p.lastCommitAt.IsZero() && time.Since(p.lastCommitAt) < readySettle {
		return
	}
	addr, _ := p.LeaderWithID()
	if addr != "" && p.appliedIndex.Load() >= p.raftLog.CommitIndex() {
		p.markReady()
	}
}

func (p *Persistence) markAppliedEntry(index uint64) error {
	err := p.db.Update(func(tx *kvTx) error {
		return markApplied(tx.Bucket(bucketName), index)
	})
	if err != nil {
		return err
	}
	p.appliedIndex.Store(index)
	return nil
}

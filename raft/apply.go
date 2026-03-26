package raft

import (
	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kode4food/timebox"
)

func (p *Persistence) handleReady(rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := p.raftLog.ApplySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if err := p.raftLog.Save(rd); err != nil {
		return err
	}
	if len(rd.Entries) != 0 {
		if err := p.raftLog.AppendEntries(rd.Entries); err != nil {
			return err
		}
	}
	if err := p.queueMessages(rd.Messages); err != nil {
		return err
	}
	if err := p.restoreSnapshot(rd.Snapshot); err != nil {
		return err
	}
	if err := p.applyCommittedEntries(rd.CommittedEntries); err != nil {
		return err
	}
	if len(rd.CommittedEntries) != 0 {
		p.requestCompact()
	}
	if p.State() == StateLeader {
		p.markReady()
	} else if !raft.IsEmptySnap(rd.Snapshot) ||
		len(rd.CommittedEntries) != 0 {
		p.markReadyFollower()
	}
	p.node.Advance()
	return nil
}

func (p *Persistence) applyCommittedEntries(ents []raftpb.Entry) error {
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
			if err := p.applyConfChange(ent); err != nil {
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
		res := results[i]
		p.resolveProposal(propIDs[i], res)
		if err := res.Error; err != nil {
			return err
		}
		p.appliedIndex.Store(batch[i].index)
	}
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
		p.resolveProposal(propIDs[i], res)
		if err := res.Error; err != nil {
			return err
		}
		if evs := p.proposalEvents(propIDs[i]); len(evs) > 0 {
			published = append(published, evs...)
		} else if res.Append != nil {
			published = append(published, res.Append.Events...)
		}
		p.appliedIndex.Store(batch[i].index)
	}
	if len(published) != 0 {
		p.Publisher(published...)
	}
	return nil
}

func (p *Persistence) applyConfChange(ent raftpb.Entry) error {
	if len(ent.Data) == 0 {
		return p.markAppliedEntry(ent.Index)
	}
	var cc raftpb.ConfChange
	if err := cc.Unmarshal(ent.Data); err != nil {
		return err
	}
	p.node.ApplyConfChange(cc)
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
	addr, _ := p.LeaderWithID()
	if addr != "" {
		p.markReady()
	}
}

func (p *Persistence) markAppliedEntry(index uint64) error {
	err := p.db.Update(func(tx *bbolt.Tx) error {
		return markApplied(tx.Bucket(bucketName), index)
	})
	if err != nil {
		return err
	}
	p.appliedIndex.Store(index)
	return nil
}

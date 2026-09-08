package raft

import (
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/kode4food/timebox"
)

func (b *Backend) handleReady(rd raft.Ready) error {
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := b.applySnapshot(rd.Snapshot); err != nil {
			return err
		}
	}
	if err := b.raftLog.Save(rd, b.compactBound()); err != nil {
		return err
	}
	if err := b.queueMessages(rd.Messages); err != nil {
		return err
	}
	if len(rd.CommittedEntries) != 0 {
		b.lastCommitAt = time.Now()
	}
	if err := b.applyCommittedEntries(rd.CommittedEntries); err != nil {
		return err
	}
	if b.State() == StateLeader {
		b.markReady()
	} else if len(rd.CommittedEntries) == 0 {
		b.markReadyFollower()
	}
	b.node.Advance()
	return nil
}

func (b *Backend) applyCommittedEntries(ents []*raftpb.Entry) error {
	return b.applyEntries(ents, b.applyConfChange)
}

func (b *Backend) applyStartupEntries(ents []*raftpb.Entry) error {
	return b.applyEntries(ents, func(ent *raftpb.Entry) error {
		return b.markAppliedEntry(ent.GetIndex())
	})
}

func (b *Backend) applyEntries(
	ents []*raftpb.Entry, confChange func(*raftpb.Entry) error,
) error {
	if len(ents) == 0 {
		return nil
	}

	var batch []decodedEntry
	var propIDs []uint64

	flushAndReset := func() error {
		if err := b.flushBatch(batch, propIDs); err != nil {
			return err
		}
		batch = batch[:0]
		propIDs = propIDs[:0]
		return nil
	}

	for _, ent := range ents {
		switch ent.GetType() {
		case raftpb.EntryConfChange,
			raftpb.EntryConfChangeV2:
			if err := flushAndReset(); err != nil {
				return err
			}
			if err := confChange(ent); err != nil {
				return err
			}
		case raftpb.EntryNormal:
			data := ent.GetData()
			if len(data) == 0 {
				if err := flushAndReset(); err != nil {
					return err
				}
				if err := b.markAppliedEntry(ent.GetIndex()); err != nil {
					return err
				}
				continue
			}
			cmd := Command(data)
			propID, err := cmd.ProposalID()
			if err != nil {
				return err
			}
			batch = append(batch, decodedEntry{
				index: ent.GetIndex(),
				cmd:   cmd,
			})
			propIDs = append(propIDs, propID)
		default:
			if err := flushAndReset(); err != nil {
				return err
			}
			if err := b.markAppliedEntry(ent.GetIndex()); err != nil {
				return err
			}
		}
	}
	return b.flushBatch(batch, propIDs)
}

func (b *Backend) flushBatchNoPublish(
	batch []decodedEntry, propIDs []uint64,
) error {
	if len(batch) == 0 {
		return nil
	}
	results, err := b.fsm.applyEntries(batch)
	if err != nil {
		return err
	}
	for i := range batch {
		b.resolveProposal(propIDs[i], batch[i].cmd, results[i])
		b.notifyAppliedArchive(batch[i].cmd, results[i])
	}
	b.appliedIndex.Store(batch[len(batch)-1].index)
	return nil
}

func (b *Backend) flushBatchPublish(
	batch []decodedEntry, propIDs []uint64,
) error {
	if len(batch) == 0 {
		return nil
	}
	results, err := b.fsm.applyEntries(batch)
	if err != nil {
		return err
	}
	var published []*timebox.Event
	for i := range batch {
		res := results[i]
		b.resolveProposal(propIDs[i], batch[i].cmd, res)
		b.notifyAppliedArchive(batch[i].cmd, res)
		if res.Error != nil {
			continue
		}
		if evs := b.proposalEvents(propIDs[i], batch[i].cmd); len(evs) > 0 {
			published = append(published, evs...)
		} else {
			for _, req := range res.Appends {
				published = append(published, req.Events...)
			}
		}
	}
	b.appliedIndex.Store(batch[len(batch)-1].index)
	if len(published) != 0 {
		b.publishQ.Put(published)
	}
	return nil
}

func (b *Backend) notifyAppliedArchive(cmd Command, res *ApplyResult) {
	if res.Error != nil {
		return
	}
	switch cmd.Type() {
	case CmdTypeArchive, CmdTypeConsumeArchive:
		b.notifyArchive()
	}
}

func (b *Backend) applyConfChange(ent *raftpb.Entry) error {
	data := ent.GetData()
	if len(data) == 0 {
		return b.markAppliedEntry(ent.GetIndex())
	}
	var cc raftpb.ConfChange
	if err := proto.Unmarshal(data, &cc); err != nil {
		return err
	}
	cs := b.node.ApplyConfChange(&cc)
	if err := b.raftLog.SetConfState(cs); err != nil {
		return err
	}
	return b.markAppliedEntry(ent.GetIndex())
}

func (b *Backend) markReady() {
	if b.cfg.Publisher != nil {
		b.flushBatch = b.flushBatchPublish
	}
	b.readyOnce.Do(func() {
		close(b.readyCh)
	})
}

func (b *Backend) markReadyFollower() {
	if !b.lastCommitAt.IsZero() && time.Since(b.lastCommitAt) < readySettle {
		return
	}
	addr, _ := b.LeaderWithID()
	if addr != "" && b.appliedIndex.Load() >= b.raftLog.CommitIndex() {
		b.markReady()
	}
}

func (b *Backend) markAppliedEntry(index uint64) error {
	err := b.db.Update(func(tx *kvTx) error {
		return markApplied(tx.Bucket(bucketName), index)
	})
	if err != nil {
		return err
	}
	b.appliedIndex.Store(index)
	return nil
}

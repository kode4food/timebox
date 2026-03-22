package raft

import (
	"errors"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
)

func (p *Persistence) queueMessages(msgs []raftpb.Message) error {
	byPeer := map[uint64]peerMessage{}
	var snaps []peerMessage
	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}
		if _, ok := p.sendChs[msg.To]; !ok {
			p.node.ReportUnreachable(msg.To)
			if msg.Type == raftpb.MsgSnap {
				p.node.ReportSnapshot(msg.To, raft.SnapshotFailure)
			}
			continue
		}

		if msg.Type == raftpb.MsgSnap {
			s, err := p.makeSnapMessage(msg)
			if err != nil {
				return err
			}
			snaps = append(snaps, s)
			continue
		}

		data, err := msg.Marshal()
		if err != nil {
			return err
		}
		s := byPeer[msg.To]
		s.to = msg.To
		s.data = append(s.data, data)
		byPeer[msg.To] = s
	}

	for _, s := range byPeer {
		ch := p.sendChs[s.to]
		select {
		case ch <- s:
		default:
			p.node.ReportUnreachable(s.to)
		}
	}

	for _, s := range snaps {
		ch := p.sendChs[s.to]
		select {
		case ch <- s:
		default:
			_ = s.snap.Close()
			p.node.ReportUnreachable(s.to)
			p.node.ReportSnapshot(s.to, raft.SnapshotFailure)
		}
	}
	return nil
}

func (p *Persistence) makeSnapMessage(
	msg raftpb.Message,
) (peerMessage, error) {
	index := p.appliedIndex.Load()
	cs := p.confState()
	sn, err := p.storage.CreateSnapshot(index, &cs, nil)
	switch {
	case err == nil:
	case errors.Is(err, raft.ErrSnapOutOfDate):
		sn, err = p.storage.Snapshot()
	default:
		return peerMessage{}, err
	}
	if err != nil {
		return peerMessage{}, err
	}
	if sn.Metadata.Index != index {
		return peerMessage{}, ErrCorruptState
	}
	snap, err := openSnapshot(p.db, index)
	if err != nil {
		return peerMessage{}, err
	}
	msg.Snapshot = &sn
	data, err := msg.Marshal()
	if err != nil {
		_ = snap.Close()
		return peerMessage{}, err
	}
	return peerMessage{
		to:   msg.To,
		data: [][]byte{data},
		snap: snap,
	}, nil
}

func (p *Persistence) compactLog() error {
	applied := p.appliedIndex.Load()
	snapshot := p.raftLog.snapshotIndex()
	if applied == 0 || applied <= snapshot {
		return nil
	}
	if applied-snapshot < p.CompactMinStep {
		return nil
	}

	compactTo := uint64(1)
	if applied > p.CompactMinStep {
		compactTo = applied - p.CompactMinStep
	}
	return p.raftLog.Compact(applied, compactTo, p.confState())
}

func (p *Persistence) confState() raftpb.ConfState {
	cfg := p.node.Status().Config
	return raftpb.ConfState{
		Voters:         cfg.Voters[0].Slice(),
		VotersOutgoing: cfg.Voters[1].Slice(),
		Learners:       quorum.MajorityConfig(cfg.Learners).Slice(),
		LearnersNext:   quorum.MajorityConfig(cfg.LearnersNext).Slice(),
		AutoLeave:      cfg.AutoLeave,
	}
}

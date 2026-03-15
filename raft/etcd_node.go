package raft

import (
	"bufio"
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"net"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

type proposalResult struct {
	res *applyResult
}

const (
	tickInterval   = 100 * time.Millisecond
	heartbeatTick  = 1
	electionTick   = 10
	maxSizePerMsg  = 1024 * 1024
	maxInflightMsg = 256
)

func (p *Persistence) startLoops() {
	p.serveTransport()
	p.serveTicks()
	p.serveReady()
}

func (p *Persistence) serveTicks() {
	t := time.NewTicker(tickInterval)
	p.bgWG.Go(func() {
		defer t.Stop()

		for {
			select {
			case <-p.stopCh:
				return
			case <-t.C:
				p.node.Tick()
			}
		}
	})
}

func (p *Persistence) serveTransport() {
	p.bgWG.Go(func() {

		for {
			conn, err := p.transport.Accept()
			if err != nil {
				if errors.Is(err, ErrTransportClosed) ||
					errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}

			p.bgWG.Go(func() {
				p.handleTransportConn(conn)
			})
		}
	})
}

func (p *Persistence) handleTransportConn(conn net.Conn) {
	defer func() {
		p.transport.releaseConn(conn)
		_ = conn.Close()
	}()

	rd := bufio.NewReader(conn)
	for {
		msg, err := readTransportMessage(rd)
		if err != nil {
			if isClosedConn(err) {
				return
			}
			return
		}
		if err := p.node.Step(context.Background(), msg); err != nil &&
			!errors.Is(err, raft.ErrStopped) {
			return
		}
	}
}

func (p *Persistence) serveReady() {
	p.bgWG.Go(func() {
		for {
			select {
			case <-p.stopCh:
				return
			case rd, ok := <-p.node.Ready():
				if !ok {
					return
				}
				if err := p.handleReady(rd); err != nil {
					p.handleLoopError(err)
					return
				}
			}
		}
	})
}

func (p *Persistence) handleReady(rd raft.Ready) error {
	if err := p.raftLog.Save(rd); err != nil {
		return err
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		return errors.New("raft snapshots are unsupported")
	}
	if len(rd.Entries) != 0 {
		if err := p.raftLog.AppendEntries(rd.Entries); err != nil {
			return err
		}
	}
	p.sendMessages(rd.Messages)
	if err := p.applyBatch(rd.CommittedEntries); err != nil {
		return err
	}
	if rd.SoftState != nil {
		p.updateLeader(rd.SoftState.Lead)
	}
	p.markReadyIfPossible()
	p.node.Advance()
	return p.maybeProposeCompaction()
}

func (p *Persistence) applyBatch(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	return p.applyCommittedEntries(append([]raftpb.Entry(nil), ents...))
}

func (p *Persistence) applyCommittedEntries(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	normal := make([]raftpb.Entry, 0, len(ents))
	flush := func() error {
		if len(normal) == 0 {
			return nil
		}

		results, err := p.fsm.ApplyEntries(normal)
		if err != nil {
			return err
		}
		for i, ent := range normal {
			res := normalizeApplyResult(results[i])
			p.resolveProposal(ent, res)
			if err := res.Err(); err != nil {
				return err
			}
			p.appliedIndex = ent.Index
		}
		normal = normal[:0]
		return nil
	}

	for _, ent := range ents {
		switch ent.Type {
		case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
			if err := flush(); err != nil {
				return err
			}
			if err := p.applyCommittedEntry(ent); err != nil {
				return err
			}
		case raftpb.EntryNormal:
			if peekCommandType(ent.Data) == commandCompact {
				if err := flush(); err != nil {
					return err
				}
				if err := p.applyCommittedEntry(ent); err != nil {
					return err
				}
				continue
			}
			normal = append(normal, ent)
		default:
			normal = append(normal, ent)
		}
	}
	return flush()
}

func (p *Persistence) sendMessages(msgs []raftpb.Message) {
	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}
		addr, ok := p.addrByNodeID[msg.To]
		if !ok || addr == "" {
			p.node.ReportUnreachable(msg.To)
			continue
		}
		err := p.transport.Send(addr, msg)
		if err != nil {
			p.node.ReportUnreachable(msg.To)
			if msg.Type == raftpb.MsgSnap {
				p.node.ReportSnapshot(msg.To, raft.SnapshotFailure)
			}
			continue
		}
		if msg.Type == raftpb.MsgSnap {
			p.node.ReportSnapshot(msg.To, raft.SnapshotFinish)
		}
	}
}

func (p *Persistence) applyCommittedEntry(ent raftpb.Entry) error {
	switch ent.Type {
	case raftpb.EntryConfChange:
		if len(ent.Data) == 0 {
			return p.markAppliedEntry(ent.Index)
		}
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(ent.Data); err != nil {
			return err
		}
		p.node.ApplyConfChange(cc)
		return p.markAppliedEntry(ent.Index)
	case raftpb.EntryConfChangeV2:
		if len(ent.Data) == 0 {
			return p.markAppliedEntry(ent.Index)
		}
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(ent.Data); err != nil {
			return err
		}
		p.node.ApplyConfChange(cc)
		return p.markAppliedEntry(ent.Index)
	default:
		if peekCommandType(ent.Data) == commandCompact {
			return p.applyCompactEntry(ent)
		}
		res := p.fsm.ApplyEntry(ent)
		p.resolveProposal(ent, res)
		if err := res.Err(); err != nil {
			return err
		}
		p.appliedIndex = ent.Index
		return nil
	}
}

func (p *Persistence) maybeProposeCompaction() error {
	if p.State() != StateLeader {
		p.clearPendingCompaction()
		return nil
	}

	compactTo := p.safeCompactIndex()
	current := p.raftLog.CompactedIndex()
	if compactTo == 0 || compactTo <= current {
		return nil
	}
	if compactTo-current < p.compactMinStep {
		return nil
	}
	if !p.beginCompaction(compactTo) {
		return nil
	}
	if err := p.proposeCompaction(compactTo); err != nil {
		p.clearPendingCompaction()
		return err
	}
	return nil
}

func (p *Persistence) applyCompactEntry(ent raftpb.Entry) error {
	cmd, err := decodeCommand(ent.Data)
	if err != nil {
		return err
	}
	if cmd.Compact == nil {
		return ErrCompactCommandMissing
	}
	if err := p.raftLog.Compact(cmd.Compact.Index, p.confState()); err != nil {
		return err
	}
	p.clearPendingCompaction()
	return p.markAppliedEntry(ent.Index)
}

func (p *Persistence) propose(
	ctx context.Context, data []byte, proposalID string,
) (*applyResult, error) {
	ch := p.registerProposal(proposalID)
	defer p.unregisterProposal(proposalID, ch)

	if err := p.node.Propose(ctx, data); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if err := res.res.Err(); err != nil {
			return nil, err
		}
		return normalizeApplyResult(res.res), nil
	}
}

func (p *Persistence) registerProposal(id string) chan proposalResult {
	ch := make(chan proposalResult, 1)
	p.pendingMu.Lock()
	p.pending[id] = ch
	p.pendingMu.Unlock()
	return ch
}

func (p *Persistence) unregisterProposal(id string, ch chan proposalResult) {
	p.pendingMu.Lock()
	if cur, ok := p.pending[id]; ok && cur == ch {
		delete(p.pending, id)
	}
	p.pendingMu.Unlock()
}

func (p *Persistence) resolveProposal(ent raftpb.Entry, res *applyResult) {
	if len(ent.Data) == 0 {
		return
	}
	proposalID := decodeProposalID(ent.Data)
	if proposalID == "" {
		return
	}

	p.pendingMu.Lock()
	ch, ok := p.pending[proposalID]
	if ok {
		delete(p.pending, proposalID)
	}
	p.pendingMu.Unlock()
	if ok {
		ch <- proposalResult{res: normalizeApplyResult(res)}
	}
}

func (p *Persistence) failAllPending(err error) {
	p.pendingMu.Lock()
	pending := p.pending
	p.pending = map[string]chan proposalResult{}
	p.pendingMu.Unlock()

	for _, ch := range pending {
		ch <- proposalResult{res: encodeApplyError(applyCodeInternal, err)}
	}
}

func (p *Persistence) handleLoopError(err error) {
	p.failAllPending(err)
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	p.node.Stop()
}

func (p *Persistence) beginCompaction(index uint64) bool {
	p.compactMu.Lock()
	defer p.compactMu.Unlock()

	if p.compactPending != 0 {
		return false
	}
	p.compactPending = index
	return true
}

func (p *Persistence) clearPendingCompaction() {
	p.compactMu.Lock()
	p.compactPending = 0
	p.compactMu.Unlock()
}

func (p *Persistence) confState() raftpb.ConfState {
	cfg := p.node.Status().Config
	return raftpb.ConfState{
		Voters:         cfg.Voters[0].Slice(),
		VotersOutgoing: cfg.Voters[1].Slice(),
		Learners:       quorum.MajorityConfig(cfg.Learners).Slice(),
		LearnersNext: quorum.MajorityConfig(
			cfg.LearnersNext,
		).Slice(),
		AutoLeave: cfg.AutoLeave,
	}
}

func (p *Persistence) proposeCompaction(index uint64) error {
	data, err := encodeCommand(command{
		Type:    commandCompact,
		Compact: &compactCommand{Index: index},
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), p.applyTimeout,
	)
	defer cancel()
	return p.node.Propose(ctx, data)
}

func (p *Persistence) safeCompactIndex() uint64 {
	if p.appliedIndex == 0 {
		return 0
	}

	st := p.node.Status()
	if st.RaftState != raft.StateLeader {
		return 0
	}

	floor := p.appliedIndex
	ids := voterIDs(st.Config)
	if len(ids) == 0 {
		return floor
	}

	for _, id := range ids {
		pr, ok := st.Progress[id]
		if !ok {
			return 0
		}
		if pr.Match < floor {
			floor = pr.Match
		}
	}
	return floor
}

func newRaftNodeConfig(
	id uint64, storage raft.Storage, applied uint64,
) *raft.Config {
	lg := slog.Default()
	raftLogger := &raft.DefaultLogger{
		Logger: slog.NewLogLogger(
			lg.With(slog.String("component", "timebox-raft")).Handler(),
			slog.LevelInfo,
		),
	}
	return &raft.Config{
		ID:              id,
		ElectionTick:    electionTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		Applied:         applied,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsg,
		PreVote:         true,
		CheckQuorum:     true,
		Logger:          raftLogger,
	}
}

func voterIDs(cfg tracker.Config) []uint64 {
	ids := make(map[uint64]struct{})
	for _, id := range cfg.Voters[0].Slice() {
		ids[id] = struct{}{}
	}
	for _, id := range cfg.Voters[1].Slice() {
		ids[id] = struct{}{}
	}

	res := make([]uint64, 0, len(ids))
	for id := range ids {
		res = append(res, id)
	}
	return res
}

func nodeID(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	v := h.Sum64()
	if v == 0 {
		return 1
	}
	return v
}

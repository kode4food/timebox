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

	"github.com/kode4food/timebox"
)

type proposalResult struct {
	res *ApplyResult
}

type proposalState struct {
	ch     chan proposalResult
	events []*timebox.Event
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
					p.pendingMu.Lock()
					pending := p.pending
					p.pending = map[uint64]proposalState{}
					p.pendingMu.Unlock()
					for _, st := range pending {
						st.ch <- proposalResult{
							res: &ApplyResult{
								Code:    applyCodeInternal,
								Message: err.Error(),
							},
						}
					}
					p.stopOnce.Do(func() {
						close(p.stopCh)
					})
					p.node.Stop()
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
	if p.State() == StateLeader {
		p.markReady()
	} else if len(rd.CommittedEntries) != 0 {
		p.markReadyFollower()
	}
	p.node.Advance()
	return p.maybeProposeCompaction()
}

func (p *Persistence) applyBatch(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	return p.applyCommittedEntries(append([]raftpb.Entry(nil), ents...))
}

func (p *Persistence) applyCommittedEntries(
	ents []raftpb.Entry,
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
			if cmd.Type() == CmdTypeCompact {
				if err := flushAndReset(); err != nil {
					return err
				}
				if err := p.applyCompact(cmd); err != nil {
					return err
				}
				if err := p.markAppliedEntry(ent.Index); err != nil {
					return err
				}
				continue
			}
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

func (p *Persistence) flushBatch(
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
		if err := res.Err(); err != nil {
			return err
		}
		if p.Publisher != nil {
			evs, err := p.committedEvents(batch[i].cmd, propIDs[i])
			if err != nil {
				return err
			}
			published = append(published, evs...)
		}
		p.appliedIndex = batch[i].index
	}
	if p.Publisher != nil && len(published) > 0 {
		p.Publisher(published...)
	}
	return nil
}

func (p *Persistence) committedEvents(
	cmd Command, proposalID uint64,
) ([]*timebox.Event, error) {
	if evs := p.proposalEvents(proposalID); len(evs) > 0 {
		return evs, nil
	}
	if cmd.Type() != CmdTypeAppend {
		return nil, nil
	}

	req, err := cmd.AppendRequest()
	if err != nil {
		return nil, err
	}
	return req.Events, nil
}

func (p *Persistence) sendMessages(msgs []raftpb.Message) {
	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}
		peer, ok := p.peers[msg.To]
		if !ok || peer.RaftAddr == "" {
			p.node.ReportUnreachable(msg.To)
			continue
		}
		if err := p.transport.Send(peer.RaftAddr, msg); err != nil {
			p.node.ReportUnreachable(msg.To)
		}
	}
}

func (p *Persistence) applyConfChange(
	ent raftpb.Entry,
) error {
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
	if compactTo-current < p.CompactMinStep {
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

func (p *Persistence) applyCompact(cmd Command) error {
	if err := p.raftLog.Compact(
		cmd.CompactIndex(), p.confState(),
	); err != nil {
		return err
	}
	p.clearPendingCompaction()
	return nil
}

func (p *Persistence) propose(
	ctx context.Context, data []byte, proposalID uint64, events []*timebox.Event,
) (*ApplyResult, error) {
	st := p.registerProposal(proposalID, events)
	defer p.unregisterProposal(proposalID, st)

	if err := p.node.Propose(ctx, data); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-st.ch:
		if err := res.res.Err(); err != nil {
			return nil, err
		}
		return res.res, nil
	}
}

func (p *Persistence) registerProposal(
	id uint64, events []*timebox.Event,
) proposalState {
	st := proposalState{
		ch:     make(chan proposalResult, 1),
		events: append([]*timebox.Event(nil), events...),
	}
	p.pendingMu.Lock()
	p.pending[id] = st
	p.pendingMu.Unlock()
	return st
}

func (p *Persistence) unregisterProposal(id uint64, st proposalState) {
	p.pendingMu.Lock()
	if cur, ok := p.pending[id]; ok && cur.ch == st.ch {
		delete(p.pending, id)
	}
	p.pendingMu.Unlock()
}

func (p *Persistence) resolveProposal(
	proposalID uint64, res *ApplyResult,
) {
	if proposalID == 0 {
		return
	}
	p.pendingMu.Lock()
	st, ok := p.pending[proposalID]
	if ok {
		delete(p.pending, proposalID)
	}
	p.pendingMu.Unlock()
	if ok {
		st.ch <- proposalResult{res: res}
	}
}

func (p *Persistence) proposalEvents(proposalID uint64) []*timebox.Event {
	if proposalID == 0 {
		return nil
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	st, ok := p.pending[proposalID]
	if !ok || len(st.events) == 0 {
		return nil
	}
	evs := st.events
	st.events = nil
	p.pending[proposalID] = st
	return evs
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
	ctx, cancel := context.WithTimeout(
		context.Background(), p.applyTimeout,
	)
	defer cancel()
	return p.node.Propose(ctx, MakeCompactCommand(0, index))
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

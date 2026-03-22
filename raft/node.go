package raft

import (
	"bufio"
	"context"
	"errors"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/quorum"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kode4food/timebox"
)

type (
	proposalResult struct {
		res *ApplyResult
	}

	proposalState struct {
		ch     chan proposalResult
		events []*timebox.Event
	}
)

const (
	tickInterval   = 100 * time.Millisecond
	heartbeatTick  = 1
	electionTick   = 10
	maxSizePerMsg  = 1024 * 1024
	maxInflightMsg = 256
)

func (p *Persistence) startLoops() {
	p.serveCompaction()
	p.servePeerSends()
	p.serveTransport()
	p.serveTicks()
	p.serveReady()
}

func (p *Persistence) serveCompaction() {
	p.bgWG.Go(func() {
		for {
			select {
			case <-p.stopCh:
				return
			case <-p.compactCh:
				if err := p.compactLog(); err != nil {
					p.stopWithError(err)
					return
				}
			}
		}
	})
}

func (p *Persistence) servePeerSends() {
	localID := nodeID(p.LocalID)
	for id, peer := range p.peers {
		if id == localID || peer.RaftAddr == "" {
			continue
		}
		ch := make(chan []raftpb.Message, maxInflightMsg)
		p.sendChs[id] = ch

		id := id
		peer := peer
		p.bgWG.Go(func() {
			p.servePeerSend(id, peer, ch)
		})
	}
}

func (p *Persistence) servePeerSend(
	id uint64, peer peerInfo, ch <-chan []raftpb.Message,
) {
	for {
		select {
		case <-p.stopCh:
			return
		case msgs := <-ch:
			if err := p.transport.Send(peer.RaftAddr, msgs...); err != nil {
				p.reportUnreachable(id)
			}
		}
	}
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
		msg, snapSize, err := readTransportMessage(rd)
		if err != nil {
			return
		}
		if msg.Type == raftpb.MsgSnap {
			if err := saveSnapshot(
				p.raftLog.snapshotDir, io.LimitReader(rd, int64(snapSize)),
				msg.Snapshot.Metadata.Index,
			); err != nil {
				return
			}
		}
		err = p.node.Step(context.Background(), msg)
		if err != nil && !errors.Is(err, raft.ErrStopped) {
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
					p.stopWithError(err)
					return
				}
			}
		}
	})
}

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
	msgs, snapMsgs := splitSnapMsgs(rd.Messages)
	p.sendMessages(msgs)
	if err := p.restoreSnapshot(rd.Snapshot); err != nil {
		return err
	}
	if err := p.applyBatch(rd.CommittedEntries); err != nil {
		return err
	}
	if len(rd.CommittedEntries) != 0 {
		p.requestCompact()
	}
	if err := p.sendSnapshots(snapMsgs); err != nil {
		return err
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

func splitSnapMsgs(msgs []raftpb.Message) ([]raftpb.Message, []raftpb.Message) {
	var out []raftpb.Message
	var snaps []raftpb.Message
	for _, msg := range msgs {
		if msg.Type == raftpb.MsgSnap {
			snaps = append(snaps, msg)
			continue
		}
		out = append(out, msg)
	}
	return out, snaps
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
		if err := res.Err(); err != nil {
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
		if err := res.Err(); err != nil {
			return err
		}
		evs, err := p.committedEvents(batch[i].cmd, propIDs[i])
		if err != nil {
			return err
		}
		published = append(published, evs...)
		p.appliedIndex.Store(batch[i].index)
	}
	if len(published) != 0 {
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
	byPeer := map[uint64][]raftpb.Message{}
	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}
		peer, ok := p.peers[msg.To]
		if !ok || peer.RaftAddr == "" {
			p.reportUnreachable(msg.To)
			continue
		}

		byPeer[msg.To] = append(byPeer[msg.To], msg)
	}

	for id, batch := range byPeer {
		ch, ok := p.sendChs[id]
		if !ok {
			p.reportUnreachable(id)
			continue
		}

		msgs := append([]raftpb.Message(nil), batch...)
		select {
		case ch <- msgs:
		default:
			p.reportUnreachable(id)
		}
	}
}

func (p *Persistence) sendSnapshots(msgs []raftpb.Message) error {
	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}
		peer, ok := p.peers[msg.To]
		if !ok || peer.RaftAddr == "" {
			p.reportUnreachable(msg.To)
			p.reportSnapshot(msg.To, raft.SnapshotFailure)
			continue
		}

		snapMsg, snap, err := p.makeSnapMsg(msg)
		if err != nil {
			return err
		}

		id := msg.To
		addr := peer.RaftAddr
		sm := snapMsg
		sn := snap
		p.bgWG.Go(func() {
			defer func() {
				_ = sn.Close()
			}()

			if err := p.transport.SendSnapshot(addr, sm, sn); err != nil {
				p.reportUnreachable(id)
				p.reportSnapshot(id, raft.SnapshotFailure)
				return
			}
			p.reportSnapshot(id, raft.SnapshotFinish)
		})
	}
	return nil
}

func (p *Persistence) reportUnreachable(id uint64) {
	select {
	case <-p.stopCh:
		return
	default:
		p.node.ReportUnreachable(id)
	}
}

func (p *Persistence) reportSnapshot(
	id uint64, status raft.SnapshotStatus,
) {
	select {
	case <-p.stopCh:
		return
	default:
		p.node.ReportSnapshot(id, status)
	}
}

func (p *Persistence) makeSnapMsg(
	msg raftpb.Message,
) (raftpb.Message, *snapshot, error) {
	index := p.appliedIndex.Load()
	cs := p.confState()
	sn, err := p.storage.CreateSnapshot(index, &cs, nil)
	switch {
	case err == nil:
	case errors.Is(err, raft.ErrSnapOutOfDate):
		sn, err = p.storage.Snapshot()
	default:
		return raftpb.Message{}, nil, err
	}
	if err != nil {
		return raftpb.Message{}, nil, err
	}
	if sn.Metadata.Index != index {
		return raftpb.Message{}, nil, ErrCorruptState
	}
	snap, err := openSnapshot(p.db, index)
	if err != nil {
		return raftpb.Message{}, nil, err
	}
	msg.Snapshot = &sn
	return msg, snap, nil
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

func (p *Persistence) requestCompact() {
	select {
	case p.compactCh <- struct{}{}:
	default:
	}
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

func (p *Persistence) stopWithError(err error) {
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
		p.node.Stop()
	})
}

func (p *Persistence) propose(
	ctx context.Context, data []byte, proposalID uint64,
	events []*timebox.Event,
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

func nodeID(id string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	v := h.Sum64()
	if v == 0 {
		return 1
	}
	return v
}

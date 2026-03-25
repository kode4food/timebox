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
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kode4food/timebox"
)

type (
	peerMessage struct {
		to   uint64
		data [][]byte
		snap *snapshot
	}

	proposalState struct {
		ch     chan *ApplyResult
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

func (p *Persistence) servePeerSends() {
	localID := nodeID(p.LocalID)
	for id, peer := range p.peers {
		if id == localID || peer.RaftAddr == "" {
			continue
		}
		ch := make(chan peerMessage, maxInflightMsg)
		p.sendChs[id] = ch

		peer := peer
		p.bgWG.Go(func() {
			p.servePeerSend(peer, ch)
		})
	}
}

func (p *Persistence) servePeerSend(peer peerInfo, ch <-chan peerMessage) {
	for {
		select {
		case <-p.stopCh:
			return
		case s := <-ch:
			if s.snap != nil {
				p.sendSnapshotAsync(peer, s)
				continue
			}
			if err := p.transport.Send(
				peer.RaftAddr, s.data...,
			); err != nil {
				p.node.ReportUnreachable(s.to)
			}
		}
	}
}

func (p *Persistence) sendSnapshotAsync(peer peerInfo, s peerMessage) {
	p.bgWG.Go(func() {
		defer func() { _ = s.snap.Close() }()
		if err := p.transport.SendSnapshot(
			peer.RaftAddr, s.data[0], s.snap,
		); err != nil {
			p.node.ReportUnreachable(s.to)
			p.node.ReportSnapshot(s.to, raft.SnapshotFailure)
			return
		}
		p.node.ReportSnapshot(s.to, raft.SnapshotFinish)
	})
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

func (p *Persistence) serveCompaction() {
	p.bgWG.Go(func() {
		for {
			select {
			case <-p.stopCh:
				return
			case <-p.compactCh:
				if err := p.compactLog(); err != nil {
					p.stop(internalError(err))
					return
				}
			}
		}
	})
}

func (p *Persistence) requestCompact() {
	select {
	case p.compactCh <- struct{}{}:
	default:
	}
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
					p.stop(internalError(err))
					return
				}
			}
		}
	})
}

func (p *Persistence) stop(err error) {
	if err == nil {
		err = raft.ErrStopped
	}
	p.stopOnce.Do(func() {
		p.stopErr.Store(err)
		close(p.stopCh)
		p.node.Stop()
	})
}

func internalError(err error) error {
	return (&ApplyResult{
		Code:    applyCodeInternal,
		Message: err.Error(),
	}).Err()
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

package raft

import (
	"bufio"
	"context"
	"errors"
	"hash/fnv"
	"log/slog"
	"net"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kode4food/timebox"
)

type (
	peerMessage struct {
		to   uint64
		data []byte
		snap bool
	}

	peerQueue struct {
		mu     sync.Mutex
		head   *peerQueueNode
		tail   *peerQueueNode
		notify chan struct{}
	}

	peerQueueNode struct {
		msg  peerMessage
		next *peerQueueNode
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
	readySettle    = 250 * time.Millisecond
)

func (p *Persistence) startLoops() {
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
		q := newPeerQueue()
		p.peerQueues[id] = q

		id := id
		peer := peer
		p.bgWG.Go(func() {
			p.servePeerSend(id, peer, q)
		})
	}
}

func (p *Persistence) queueMessages(msgs []raftpb.Message) error {
	for _, msg := range msgs {
		if msg.To == 0 {
			continue
		}

		q := p.peerQueues[msg.To]
		if q == nil {
			if msg.Type == raftpb.MsgSnap {
				p.node.ReportSnapshot(msg.To, raft.SnapshotFailure)
			}
			p.node.ReportUnreachable(msg.To)
			continue
		}

		data, err := msg.Marshal()
		if err != nil {
			return err
		}
		q.Put(peerMessage{
			to:   msg.To,
			data: data,
			snap: msg.Type == raftpb.MsgSnap,
		})
	}
	return nil
}

func (p *Persistence) servePeerSend(id uint64, peer peerInfo, q *peerQueue) {
	for {
		select {
		case <-p.stopCh:
			return
		case <-q.Ready():
			var hadSnap bool
			err := p.transport.WithPeer(
				peer.RaftAddr,
				func(w *bufio.Writer) error {
					return q.Drain(
						func(m peerMessage) error {
							if m.snap {
								hadSnap = true
							}
							return writeFrame(w, m.data)
						},
					)
				},
			)
			if err != nil {
				if hadSnap {
					p.node.ReportSnapshot(id, raft.SnapshotFailure)
				}
				p.node.ReportUnreachable(id)
				q.Signal()
			} else if hadSnap {
				p.node.ReportSnapshot(id, raft.SnapshotFinish)
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
		msg, err := readTransportMessage(rd)
		if err != nil {
			return
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
		Error: errors.Join(ErrUnexpectedApplyResult, err),
	}).Error
}

func newRaftNodeConfig(
	id uint64, storage raft.Storage, applied uint64,
) *raft.Config {
	lg := slog.Default()
	raftLogger := &raft.DefaultLogger{
		Logger: slog.NewLogLogger(
			lg.With(
				slog.String("component", "timebox-raft"),
			).Handler(),
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

func newPeerQueue() *peerQueue {
	return &peerQueue{
		notify: make(chan struct{}, 1),
	}
}

func (q *peerQueue) Put(msg peerMessage) {
	n := &peerQueueNode{msg: msg}

	q.mu.Lock()
	if q.tail == nil {
		q.head = n
		q.tail = n
	} else {
		q.tail.next = n
		q.tail = n
	}
	q.mu.Unlock()
	q.Signal()
}

func (q *peerQueue) Ready() <-chan struct{} {
	return q.notify
}

func (q *peerQueue) Drain(fn func(peerMessage) error) error {
	q.mu.Lock()
	head := q.head
	q.head = nil
	q.tail = nil
	q.mu.Unlock()

	for n := head; n != nil; n = n.next {
		if err := fn(n.msg); err != nil {
			return err
		}
	}
	return nil
}

func (q *peerQueue) Signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

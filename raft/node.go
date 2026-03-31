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
		to      uint64
		data    []byte
		snap    bool
		snapRef uint64
	}

	peerQueue struct {
		mu     sync.Mutex
		head   *peerQueueNode
		tail   *peerQueueNode
		notify chan struct{}
		len    int
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

func newPeerQueue() *peerQueue {
	return &peerQueue{
		notify: make(chan struct{}, 1),
	}
}

func (p *Persistence) startLoops() {
	p.servePublish()
	p.servePeerSends()
	p.serveTransport()
	p.serveTicks()
	p.serveReady()
}

func (p *Persistence) servePublish() {
	if p.publishQ == nil {
		return
	}
	p.bgWG.Go(func() {
		for {
			select {
			case <-p.stopCh:
				for {
					events, ok := p.publishQ.Pop()
					if !ok {
						return
					}
					p.Publisher(events...)
				}
			case <-p.publishQ.Ready():
				for {
					events, ok := p.publishQ.Pop()
					if !ok {
						break
					}
					p.Publisher(events...)
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
		var snapRef uint64
		if msg.Type == raftpb.MsgSnap {
			var ok bool
			snapRef, ok = decodeSnapshotRef(msg.Snapshot.Data)
			if !ok {
				return raft.ErrSnapshotTemporarilyUnavailable
			}
		}
		q.Put(peerMessage{
			to:      msg.To,
			data:    data,
			snap:    msg.Type == raftpb.MsgSnap,
			snapRef: snapRef,
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
			err := p.sendPeerQueue(id, peer, q)
			if err != nil {
				p.node.ReportUnreachable(id)
				q.Signal()
			}
		}
	}
}

func (p *Persistence) sendPeerQueue(
	id uint64, peer peerInfo, q *peerQueue,
) error {
	var (
		hadSnap bool
		sent    []uint64
	)

	err := p.transport.WithPeer(
		peer.RaftAddr,
		func(w *bufio.Writer) error {
			for {
				msg, ok := q.Peek()
				if !ok {
					return nil
				}
				if msg.snap {
					hadSnap = true
				}
				if err := writeFrame(w, msg.data); err != nil {
					return err
				}
				if msg.snap {
					if err := p.writeSnapshotStream(w, msg.snapRef); err != nil {
						return err
					}
					sent = append(sent, msg.snapRef)
				}
				q.Drop()
			}
		},
	)
	if err != nil {
		if hadSnap {
			p.node.ReportSnapshot(id, raft.SnapshotFailure)
		}
		return err
	}
	for _, ref := range sent {
		p.releaseOutgoingSnapshot(ref)
	}
	if hadSnap {
		p.node.ReportSnapshot(id, raft.SnapshotFinish)
	}
	return nil
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
		msg, err := p.readTransportMessage(rd)
		if err != nil {
			return
		}
		err = p.node.Step(context.Background(), msg)
		if err != nil && !errors.Is(err, raft.ErrStopped) {
			return
		}
	}
}

func (p *Persistence) readTransportMessage(
	r *bufio.Reader,
) (raftpb.Message, error) {
	data, err := readFrame(r)
	if err != nil {
		return raftpb.Message{}, err
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		return raftpb.Message{}, err
	}
	if msg.Type != raftpb.MsgSnap {
		return msg, nil
	}
	ref, ok := decodeSnapshotRef(msg.Snapshot.Data)
	if !ok {
		return raftpb.Message{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	size, err := readSnapshotSize(r)
	if err != nil {
		return raftpb.Message{}, err
	}
	if err := p.readSnapshotStream(r, ref, size); err != nil {
		return raftpb.Message{}, err
	}
	return msg, nil
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
	q.len++
	q.mu.Unlock()
	q.Signal()
}

func (q *peerQueue) Ready() <-chan struct{} {
	return q.notify
}

func (q *peerQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.len
}

func (q *peerQueue) Peek() (peerMessage, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		return peerMessage{}, false
	}
	return q.head.msg, true
}

func (q *peerQueue) Drop() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		return
	}
	q.head = q.head.next
	q.len--
	if q.head == nil {
		q.tail = nil
	}
}

func (q *peerQueue) Signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func internalError(err error) error {
	return (&ApplyResult{
		Error: errors.Join(ErrUnexpectedApplyResult, err),
	}).Error
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

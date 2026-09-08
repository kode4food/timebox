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
	"google.golang.org/protobuf/proto"

	"github.com/kode4food/timebox"
)

type (
	peerMessage struct {
		data    []byte
		to      uint64
		snapRef uint64
		snap    bool
	}

	peerQueue struct {
		head   *peerQueueNode
		tail   *peerQueueNode
		notify chan struct{}
		len    int
		mu     sync.Mutex
	}

	peerQueueNode struct {
		msg  peerMessage
		next *peerQueueNode
	}

	proposalState struct {
		ch     chan *ApplyResult
		cmd    Command
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

func (b *Backend) startLoops() {
	b.servePublish()
	b.servePeerSends()
	b.serveTransport()
	b.serveTicks()
	b.serveReady()
}

func (b *Backend) servePublish() {
	if b.publishQ == nil {
		return
	}
	b.bgWG.Go(func() {
		for {
			select {
			case <-b.stopCh:
				for {
					events, ok := b.publishQ.Pop()
					if !ok {
						return
					}
					b.cfg.Publisher(events...)
				}
			case <-b.publishQ.Ready():
				for {
					events, ok := b.publishQ.Pop()
					if !ok {
						break
					}
					b.cfg.Publisher(events...)
				}
			}
		}
	})
}

func (b *Backend) servePeerSends() {
	localID := nodeID(b.cfg.LocalID)
	for id, peer := range b.peers {
		if id == localID || peer.RaftAddr == "" {
			continue
		}
		q := newPeerQueue()
		b.peerQueues[id] = q

		id := id
		peer := peer
		b.bgWG.Go(func() {
			b.servePeerSend(id, peer, q)
		})
	}
}

func (b *Backend) queueMessages(msgs []*raftpb.Message) error {
	for _, msg := range msgs {
		to := msg.GetTo()
		if to == 0 {
			continue
		}

		typ := msg.GetType()
		q := b.peerQueues[to]
		if q == nil {
			if typ == raftpb.MsgSnap {
				b.node.ReportSnapshot(to, raft.SnapshotFailure)
			}
			b.node.ReportUnreachable(to)
			continue
		}

		data, err := proto.Marshal(msg)
		if err != nil {
			return err
		}
		var snapRef uint64
		snap := typ == raftpb.MsgSnap
		if snap {
			var ok bool
			snapRef, ok = decodeSnapshotRef(msg.GetSnapshot().GetData())
			if !ok {
				return raft.ErrSnapshotTemporarilyUnavailable
			}
		}
		q.Put(peerMessage{
			to:      to,
			data:    data,
			snap:    snap,
			snapRef: snapRef,
		})
	}
	return nil
}

func (b *Backend) servePeerSend(id uint64, peer peerInfo, q *peerQueue) {
	for {
		select {
		case <-b.stopCh:
			return
		case <-q.Ready():
			err := b.sendPeerQueue(id, peer, q)
			if err != nil {
				b.node.ReportUnreachable(id)
				q.Signal()
			}
		}
	}
}

func (b *Backend) sendPeerQueue(
	id uint64, peer peerInfo, q *peerQueue,
) error {
	var (
		hadSnap bool
		sent    []uint64
	)

	err := b.transport.WithPeer(
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
					if err := b.writeSnapshotStream(w, msg.snapRef); err != nil {
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
			b.node.ReportSnapshot(id, raft.SnapshotFailure)
		}
		return err
	}
	for _, ref := range sent {
		b.releaseOutgoingSnapshot(ref)
	}
	if hadSnap {
		b.node.ReportSnapshot(id, raft.SnapshotFinish)
	}
	return nil
}

func (b *Backend) serveTicks() {
	t := time.NewTicker(tickInterval)
	b.bgWG.Go(func() {
		defer t.Stop()

		for {
			select {
			case <-b.stopCh:
				return
			case <-t.C:
				b.node.Tick()
			}
		}
	})
}

func (b *Backend) serveTransport() {
	b.bgWG.Go(func() {
		for {
			conn, err := b.transport.Accept()
			if err != nil {
				if errors.Is(err, ErrTransportClosed) ||
					errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}

			b.bgWG.Go(func() {
				b.handleTransportConn(conn)
			})
		}
	})
}

func (b *Backend) handleTransportConn(conn net.Conn) {
	defer func() {
		b.transport.releaseConn(conn)
		_ = conn.Close()
	}()

	rd := bufio.NewReader(conn)
	for {
		msg, err := b.readTransportMessage(rd)
		if err != nil {
			return
		}
		err = b.node.Step(context.Background(), msg)
		if err != nil && !errors.Is(err, raft.ErrStopped) {
			return
		}
	}
}

func (b *Backend) readTransportMessage(
	r *bufio.Reader,
) (*raftpb.Message, error) {
	data, err := readFrame(r)
	if err != nil {
		return nil, err
	}
	msg := new(raftpb.Message)
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, err
	}
	if msg.GetType() != raftpb.MsgSnap {
		return msg, nil
	}
	ref, ok := decodeSnapshotRef(msg.GetSnapshot().GetData())
	if !ok {
		return nil, raft.ErrSnapshotTemporarilyUnavailable
	}
	size, err := readSnapshotSize(r)
	if err != nil {
		return nil, err
	}
	if err := b.readSnapshotStream(r, ref, size); err != nil {
		return nil, err
	}
	return msg, nil
}

func (b *Backend) serveReady() {
	b.bgWG.Go(func() {
		for {
			select {
			case <-b.stopCh:
				return
			case rd, ok := <-b.node.Ready():
				if !ok {
					return
				}
				if err := b.handleReady(rd); err != nil {
					slog.Error(
						"Raft ready loop stopped",
						slog.String("local_id", b.cfg.LocalID),
						slog.Any("error", err),
					)
					b.stop(internalError(err))
					return
				}
			}
		}
	})
}

func (b *Backend) stop(err error) {
	if err == nil {
		err = raft.ErrStopped
	}
	b.stopOnce.Do(func() {
		b.stopErr.Store(err)
		close(b.stopCh)
		b.node.Stop()
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

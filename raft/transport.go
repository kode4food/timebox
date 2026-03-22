package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

type (
	raftTransport struct {
		advertise ServerAddress
		listener  *net.TCPListener
		mu        sync.Mutex
		peers     map[ServerAddress]*raftPeerConn
		conns     map[net.Conn]struct{}
		closed    bool
	}

	raftPeerConn struct {
		conn net.Conn
		wr   *bufio.Writer
		mu   sync.Mutex
	}
)

const (
	defaultDialTimeout      = time.Second
	defaultKeepAlive        = 30 * time.Second
	defaultWriteTimeout     = 200 * time.Millisecond
	defaultSnapWriteTimeout = 30 * time.Second
)

var ErrTransportClosed = errors.New("transport closed")

func (t *raftTransport) Accept() (net.Conn, error) {
	conn, err := t.listener.Accept()
	if err != nil {
		return nil, err
	}
	t.trackConn(conn)
	return conn, nil
}

func (t *raftTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	peers := make([]*raftPeerConn, 0, len(t.peers))
	for _, peer := range t.peers {
		peers = append(peers, peer)
	}
	t.peers = nil
	conns := make([]net.Conn, 0, len(t.conns))
	for conn := range t.conns {
		conns = append(conns, conn)
	}
	t.conns = nil
	ln := t.listener
	t.mu.Unlock()

	var errs []error
	errs = append(errs, ln.Close())
	for _, peer := range peers {
		errs = append(errs, peer.Close())
	}
	for _, conn := range conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

func (t *raftTransport) ServerAddress() ServerAddress {
	if t.advertise != "" {
		return t.advertise
	}
	return ServerAddress(t.listener.Addr().String())
}

func (t *raftTransport) Send(addr ServerAddress, msgs ...raftpb.Message) error {
	peer, err := t.peer(addr)
	if err != nil {
		return err
	}

	if err := peer.Send(msgs...); err == nil {
		return nil
	}
	t.dropPeer(addr, peer)

	peer, err = t.peer(addr)
	if err != nil {
		return err
	}
	if err := peer.Send(msgs...); err != nil {
		t.dropPeer(addr, peer)
		return err
	}
	return nil
}

func (t *raftTransport) SendSnapshot(
	addr ServerAddress, msg raftpb.Message, snap *snapshot,
) error {
	peer, err := t.peer(addr)
	if err != nil {
		return err
	}

	if err := peer.SendSnapshot(msg, snap); err == nil {
		return nil
	}
	t.dropPeer(addr, peer)

	peer, err = t.peer(addr)
	if err != nil {
		return err
	}
	if err := peer.SendSnapshot(msg, snap); err != nil {
		t.dropPeer(addr, peer)
		return err
	}
	return nil
}

func (t *raftTransport) releaseConn(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

func (t *raftTransport) trackConn(conn net.Conn) {
	t.mu.Lock()
	t.conns[conn] = struct{}{}
	t.mu.Unlock()
}

func (t *raftTransport) peer(addr ServerAddress) (*raftPeerConn, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, ErrTransportClosed
	}
	if peer, ok := t.peers[addr]; ok {
		t.mu.Unlock()
		return peer, nil
	}
	t.mu.Unlock()

	peer, err := newRaftPeerConn(addr)
	if err != nil {
		return nil, err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		_ = peer.Close()
		return nil, ErrTransportClosed
	}
	if current, ok := t.peers[addr]; ok {
		_ = peer.Close()
		return current, nil
	}
	t.peers[addr] = peer
	return peer, nil
}

func (t *raftTransport) dropPeer(addr ServerAddress, peer *raftPeerConn) {
	t.mu.Lock()
	if current, ok := t.peers[addr]; ok && current == peer {
		delete(t.peers, addr)
	}
	t.mu.Unlock()
	_ = peer.Close()
}

func (p *raftPeerConn) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		return nil
	}
	err := p.conn.Close()
	p.conn = nil
	p.wr = nil
	return err
}

func (p *raftPeerConn) Send(msgs ...raftpb.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil || p.wr == nil {
		return ErrTransportClosed
	}
	for _, msg := range msgs {
		timeout := defaultWriteTimeout
		if msg.Type == raftpb.MsgSnap {
			timeout = defaultSnapWriteTimeout
		}
		_ = p.conn.SetDeadline(time.Now().Add(timeout))
		data, err := msg.Marshal()
		if err != nil {
			return err
		}
		if err := writeFrame(p.wr, data); err != nil {
			return err
		}
	}
	return p.wr.Flush()
}

func (p *raftPeerConn) SendSnapshot(
	msg raftpb.Message, snap *snapshot,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil || p.wr == nil {
		return ErrTransportClosed
	}
	_ = p.conn.SetDeadline(time.Now().Add(defaultSnapWriteTimeout))

	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	if err := writeFrame(p.wr, data); err != nil {
		return err
	}
	if err := binary.Write(
		p.wr, binary.BigEndian, uint64(snap.Size()),
	); err != nil {
		return err
	}
	if err := p.wr.Flush(); err != nil {
		return err
	}
	_, err = snap.WriteTo(p.conn)
	return err
}

func newRaftTransport(cfg Config) (*raftTransport, error) {
	ln, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return nil, err
	}

	t := &raftTransport{
		advertise: ServerAddress(cfg.Address),
		listener:  ln.(*net.TCPListener),
		peers:     map[ServerAddress]*raftPeerConn{},
		conns:     map[net.Conn]struct{}{},
	}

	if t.advertise == "" {
		t.advertise = ServerAddress(t.listener.Addr().String())
	}
	return t, nil
}

func newRaftPeerConn(addr ServerAddress) (*raftPeerConn, error) {
	conn, err := net.DialTimeout(
		"tcp", string(addr), defaultDialTimeout,
	)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(defaultKeepAlive)
	}
	return &raftPeerConn{
		conn: conn,
		wr:   bufio.NewWriter(conn),
	}, nil
}

func readTransportMessage(r *bufio.Reader) (raftpb.Message, uint64, error) {
	data, err := readFrame(r)
	if err != nil {
		return raftpb.Message{}, 0, err
	}
	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		return raftpb.Message{}, 0, err
	}
	if msg.Type != raftpb.MsgSnap {
		return msg, 0, nil
	}
	var size uint64
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return raftpb.Message{}, 0, err
	}
	return msg, size, nil
}

func writeFrame(w *bufio.Writer, data []byte) error {
	size := uint32(len(data))
	if err := binary.Write(w, binary.BigEndian, size); err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}

	data := make([]byte, size)
	_, err := io.ReadFull(r, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

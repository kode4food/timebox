package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
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
	defaultDialTimeout  = time.Second
	defaultKeepAlive    = 30 * time.Second
	defaultWriteTimeout = 2 * time.Second
)

var (
	ErrTransportClosed = errors.New("transport closed")
)

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

func (t *raftTransport) WithPeer(
	addr ServerAddress, fn func(*bufio.Writer) error,
) error {
	peer, err := t.peer(addr)
	if err != nil {
		return err
	}
	if err := peer.do(fn); err != nil {
		t.dropPeer(addr, peer)
		return err
	}
	return nil
}

func (t *raftTransport) releaseConn(conn net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.conns, conn)
}

func (t *raftTransport) trackConn(conn net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conns[conn] = struct{}{}
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
	defer t.mu.Unlock()
	if current, ok := t.peers[addr]; ok && current == peer {
		delete(t.peers, addr)
	}
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

func (p *raftPeerConn) do(fn func(*bufio.Writer) error) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil || p.wr == nil {
		return ErrTransportClosed
	}
	_ = p.conn.SetDeadline(
		time.Now().Add(defaultWriteTimeout),
	)
	if err := fn(p.wr); err != nil {
		return err
	}
	return p.wr.Flush()
}

func writeSnapshotFile(w io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	if err := writeSnapshotSize(w, uint64(info.Size())); err != nil {
		return err
	}
	_, err = io.Copy(w, f)
	return err
}

func writeFrame(w *bufio.Writer, data []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func readFrame(r *bufio.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(hdr[:])
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

func writeSnapshotSize(w io.Writer, size uint64) error {
	var hdr [8]byte
	binary.BigEndian.PutUint64(hdr[:], size)
	_, err := w.Write(hdr[:])
	return err
}

func readSnapshotSize(r *bufio.Reader) (uint64, error) {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(hdr[:]), nil
}

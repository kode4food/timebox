package raft

import "net"

// Transport exposes raftTransport so tests can drive peer connections
type Transport = raftTransport

// DefaultUserTimeout exposes defaultUserTimeout
const DefaultUserTimeout = defaultUserTimeout

var (
	// LogRotateBytesPtr exposes logRotateBytes so tests can override it
	LogRotateBytesPtr = &logRotateBytes

	// NewTransport exposes newRaftTransport
	NewTransport = newRaftTransport
)

// PeerCount reports how many outbound peer connections the transport holds
func (t *raftTransport) PeerCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.peers)
}

// PeerConn returns the outbound connection the transport holds for a peer
func (t *raftTransport) PeerConn(addr ServerAddress) net.Conn {
	t.mu.Lock()
	peer, ok := t.peers[addr]
	t.mu.Unlock()
	if !ok {
		return nil
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.conn
}

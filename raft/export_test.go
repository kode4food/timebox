package raft

// Transport exposes raftTransport so tests can drive peer connections
type Transport = raftTransport

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

package raft_test

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox/raft"
)

const (
	peerWatchWait  = 3 * time.Second
	peerWatchTick  = 50 * time.Millisecond
	listenDialWait = time.Second
)

func TestTransportPeer(t *testing.T) {
	t.Run("dropped when the remote closes", func(t *testing.T) {
		tr, conn := newPeerPair(t)
		assert.NoError(t, conn.Close())
		assert.Eventually(t, func() bool {
			return tr.PeerCount() == 0
		}, peerWatchWait, peerWatchTick)
	})

	t.Run("kept while the remote stays open", func(t *testing.T) {
		tr, _ := newPeerPair(t)
		assert.Never(t, func() bool {
			return tr.PeerCount() == 0
		}, peerWatchWait, peerWatchTick)
	})
}

func TestTransportListen(t *testing.T) {
	_, port, err := net.SplitHostPort(freeAddr(t))
	if !assert.NoError(t, err) {
		return
	}
	addr := net.JoinHostPort("peer.invalid", port)
	tr, err := raft.NewTransport(raft.Config{Address: addr})
	if !assert.NoError(t, err) {
		return
	}
	t.Cleanup(func() { _ = tr.Close() })

	assert.Equal(t, raft.ServerAddress(addr), tr.ServerAddress())
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), listenDialWait)
	if assert.NoError(t, err) {
		_ = conn.Close()
	}
}

// newPeerPair connects a transport to a plain listener and returns the accepted remote end
func newPeerPair(t *testing.T) (*raft.Transport, net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() { _ = ln.Close() })

	tr, err := raft.NewTransport(raft.Config{Address: freeAddr(t)})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() { _ = tr.Close() })

	err = tr.WithPeer(raft.ServerAddress(ln.Addr().String()), func(w *bufio.Writer) error {
		_, err := w.WriteString("ping")
		return err
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	conn, err := ln.Accept()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() { _ = conn.Close() })
	return tr, conn
}

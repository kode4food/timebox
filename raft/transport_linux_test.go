//go:build linux

package raft_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"

	"github.com/kode4food/timebox/raft"
)

func TestTransportUserTimeout(t *testing.T) {
	tr, remote := newPeerPair(t)
	addr := raft.ServerAddress(remote.LocalAddr().String())

	conn, ok := tr.PeerConn(addr).(*net.TCPConn)
	if !assert.True(t, ok) {
		t.FailNow()
	}
	rc, err := conn.SyscallConn()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	var got int
	var optErr error
	assert.NoError(t, rc.Control(func(fd uintptr) {
		got, optErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT)
	}))
	assert.NoError(t, optErr)
	assert.Equal(t, int(raft.DefaultUserTimeout.Milliseconds()), got)
}

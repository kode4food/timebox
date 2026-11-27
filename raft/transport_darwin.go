//go:build darwin

package raft

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

// setUserTimeout closes a peer connection once its retransmissions outlast the timeout, such as
// when the peer vanished without closing it. Writes alone would otherwise queue into it for minutes
func setUserTimeout(_, _ string, c syscall.RawConn) error {
	var err error
	ctlErr := c.Control(func(fd uintptr) {
		err = unix.SetsockoptInt(
			int(fd), unix.IPPROTO_TCP, unix.TCP_RXT_CONNDROPTIME,
			int(defaultUserTimeout.Seconds()),
		)
	})
	return errors.Join(ctlErr, err)
}

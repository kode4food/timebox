//go:build linux

package raft

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

// setUserTimeout closes a peer connection whose sent data goes unacknowledged, such as when the
// peer vanished without closing it. Writes alone would otherwise queue into it for minutes
func setUserTimeout(_, _ string, c syscall.RawConn) error {
	var err error
	ctlErr := c.Control(func(fd uintptr) {
		err = unix.SetsockoptInt(
			int(fd), unix.IPPROTO_TCP, unix.TCP_USER_TIMEOUT,
			int(defaultUserTimeout.Milliseconds()),
		)
	})
	return errors.Join(ctlErr, err)
}

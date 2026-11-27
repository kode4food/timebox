//go:build !linux && !darwin

package raft

import "syscall"

// setUserTimeout is a no-op where no peer connection timeout is available
func setUserTimeout(_, _ string, _ syscall.RawConn) error {
	return nil
}

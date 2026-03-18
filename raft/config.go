package raft

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/kode4food/timebox"
)

type (
	// Config defines one opinionated etcd Raft + bbolt persistence node
	Config struct {
		Timebox timebox.Config

		// Local state
		LocalID string
		DataDir string

		// Cluster identity
		Address string
		Servers []Server

		// CompactMinStep is the minimum number of new log entries required
		// before a compaction is triggered. Defaults to 16,384
		CompactMinStep uint64
	}

	// Server identifies one voter in the bootstrap configuration
	Server struct {
		ID      string
		Address string
	}
)

const (
	defaultApplyTimeout   = 10 * time.Second
	defaultCompactMinStep = 16_384
	defaultSnapshotRetain = 1
)

var (
	// ErrLocalIDRequired indicates the local Raft server ID is required
	ErrLocalIDRequired = errors.New("raft local ID is required")

	// ErrDataDirRequired indicates durable local storage must be configured
	ErrDataDirRequired = errors.New("raft data directory is required")

	// ErrAddressRequired indicates the Raft TCP listener is required
	ErrAddressRequired = errors.New("raft address is required")

	// ErrInvalidAddress indicates a raft address must be a valid host:port
	ErrInvalidAddress = errors.New("raft address must be a valid host:port")

	// ErrBootstrapMissingLocalServer indicates the bootstrap voter set must
	// include the local node
	ErrBootstrapMissingLocalServer = errors.New(
		"bootstrap servers must include the local raft ID",
	)
)

// DefaultConfig returns the opinionated defaults for one Raft node
func DefaultConfig() Config {
	return Config{
		Timebox:        timebox.DefaultConfig(),
		CompactMinStep: defaultCompactMinStep,
	}
}

// With merges another config into this config
func (c Config) With(other Config) Config {
	c.Timebox = timebox.Configure(c.Timebox, other.Timebox)
	if other.LocalID != "" {
		c.LocalID = other.LocalID
	}
	if other.Address != "" {
		c.Address = other.Address
	}
	if other.DataDir != "" {
		c.DataDir = other.DataDir
	}
	if len(other.Servers) != 0 {
		c.Servers = append([]Server(nil), other.Servers...)
	}
	if other.CompactMinStep != 0 {
		c.CompactMinStep = other.CompactMinStep
	}
	return c
}

// Validate checks whether the config is usable
func (c Config) Validate() error {
	switch {
	case c.LocalID == "":
		return ErrLocalIDRequired
	case c.DataDir == "":
		return ErrDataDirRequired
	case c.Address == "":
		return ErrAddressRequired
	}
	if _, _, err := parseAddress(c.Address); err != nil {
		return err
	}
	for _, srv := range c.Servers {
		if _, _, err := parseAddress(srv.Address); err != nil {
			return err
		}
	}
	if len(c.Servers) != 0 &&
		!containsLocalServer(c.Servers, c.LocalID) {
		return ErrBootstrapMissingLocalServer
	}
	return c.Timebox.Validate()
}

// ServerAddress returns the peer-visible Raft address
func (c Config) ServerAddress() string {
	return c.Address
}

// LocalServer returns the local server entry derived from this config
func (c Config) LocalServer() Server {
	return Server{
		ID:      c.LocalID,
		Address: c.ServerAddress(),
	}
}

func containsLocalServer(srvs []Server, localID string) bool {
	for _, srv := range srvs {
		if srv.ID == localID {
			return true
		}
	}
	return false
}

func parseAddress(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("%w: %q", ErrInvalidAddress, addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65_535 {
		return "", 0, fmt.Errorf("%w: %q", ErrInvalidAddress, addr)
	}
	return host, port, nil
}

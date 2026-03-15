package raft

import (
	"errors"
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

		// Network addresses

		// BindAddress is the TCP address this node listens on
		BindAddress string

		// AdvertiseAddress is the TCP address peers use to reach this node
		AdvertiseAddress string

		// ForwardBindAddress is the TCP address this node listens on for
		// internal follower-to-leader forwarding
		ForwardBindAddress string

		// ForwardAdvertiseAddress is the TCP address peers use to
		// forward writes to this node
		ForwardAdvertiseAddress string

		// Cluster bootstrap

		// Bootstrap initializes a new cluster when no Raft state exists yet
		Bootstrap bool

		// Servers defines the initial voter set for bootstrap
		Servers []Server

		// Runtime behavior

		ApplyTimeout time.Duration

		// CompactMinStep is the minimum replicated progress required before
		// proposing a WAL compaction marker
		CompactMinStep int

		SnapshotRetain int
	}

	// Server identifies one voter in the bootstrap configuration
	Server struct {
		ID             string
		Address        string
		ForwardAddress string
	}
)

const (
	// DefaultApplyTimeout bounds one replicated mutation round trip
	DefaultApplyTimeout   = 10 * time.Second
	DefaultCompactMinStep = 16_384
	DefaultSnapshotRetain = 2
)

var (
	// ErrLocalIDRequired indicates the local Raft server ID is required
	ErrLocalIDRequired = errors.New("raft local ID is required")

	// ErrDataDirRequired indicates durable local storage must be configured
	ErrDataDirRequired = errors.New("raft data directory is required")

	// ErrBindAddressRequired indicates the Raft TCP listener is required
	ErrBindAddressRequired = errors.New("raft bind address is required")

	// ErrForwardBindAddressRequired indicates the forward listener is required
	// for multi-node clusters
	ErrForwardBindAddressRequired = errors.New(
		"raft forward bind address is required for multi-node clusters",
	)

	// ErrInvalidApplyTimeout indicates writes require a positive timeout
	ErrInvalidApplyTimeout = errors.New(
		"raft apply timeout must be positive",
	)

	// ErrInvalidCompactMinStep indicates WAL compaction step must be positive
	ErrInvalidCompactMinStep = errors.New(
		"raft compact min step must be positive",
	)

	// ErrInvalidSnapshotRetain indicates snapshot retention must be positive
	ErrInvalidSnapshotRetain = errors.New(
		"raft snapshot retain must be positive",
	)

	// ErrBootstrapMissingLocalServer indicates the bootstrap voter set must
	// include the local node
	ErrBootstrapMissingLocalServer = errors.New(
		"bootstrap servers must include the local raft ID",
	)

	// ErrForwardServerAddressRequired indicates every server in a multi-node
	// bootstrap configuration must advertise a forward address
	ErrForwardServerAddressRequired = errors.New(
		"bootstrap servers must include forward addresses",
	)
)

// DefaultConfig returns the opinionated defaults for one Raft node
func DefaultConfig() Config {
	return Config{
		Timebox:        timebox.DefaultConfig(),
		ApplyTimeout:   DefaultApplyTimeout,
		CompactMinStep: DefaultCompactMinStep,
		SnapshotRetain: DefaultSnapshotRetain,
	}
}

// With merges another config into this config
func (c Config) With(other Config) Config {
	c.Timebox = timebox.Configure(c.Timebox, other.Timebox)
	if other.LocalID != "" {
		c.LocalID = other.LocalID
	}
	if other.BindAddress != "" {
		c.BindAddress = other.BindAddress
	}
	if other.AdvertiseAddress != "" {
		c.AdvertiseAddress = other.AdvertiseAddress
	}
	if other.ForwardBindAddress != "" {
		c.ForwardBindAddress = other.ForwardBindAddress
	}
	if other.ForwardAdvertiseAddress != "" {
		c.ForwardAdvertiseAddress = other.ForwardAdvertiseAddress
	}
	if other.DataDir != "" {
		c.DataDir = other.DataDir
	}
	if other.Bootstrap {
		c.Bootstrap = true
	}
	if len(other.Servers) != 0 {
		c.Servers = append([]Server(nil), other.Servers...)
	}
	if other.ApplyTimeout != 0 {
		c.ApplyTimeout = other.ApplyTimeout
	}
	if other.CompactMinStep != 0 {
		c.CompactMinStep = other.CompactMinStep
	}
	if other.SnapshotRetain != 0 {
		c.SnapshotRetain = other.SnapshotRetain
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
	case c.BindAddress == "":
		return ErrBindAddressRequired
	case c.ForwardAdvertiseAddress != "" && c.ForwardBindAddress == "":
		return ErrForwardBindAddressRequired
	case c.ApplyTimeout <= 0:
		return ErrInvalidApplyTimeout
	case c.CompactMinStep <= 0:
		return ErrInvalidCompactMinStep
	case c.SnapshotRetain <= 0:
		return ErrInvalidSnapshotRetain
	case len(c.Servers) > 1 && c.ForwardBindAddress == "":
		return ErrForwardBindAddressRequired
	case c.Bootstrap && len(c.Servers) != 0 &&
		!containsLocalServer(c.Servers, c.LocalID):
		return ErrBootstrapMissingLocalServer
	case len(c.Servers) > 1 && !allForwardAddressesSet(c.Servers):
		return ErrForwardServerAddressRequired
	}
	return c.Timebox.Validate()
}

// ServerAddress returns the advertised Raft address when set, else the bind
// address
func (c Config) ServerAddress() string {
	if c.AdvertiseAddress != "" {
		return c.AdvertiseAddress
	}
	return c.BindAddress
}

// ForwardAddress returns the advertised forward address when set, else the
// bind address
func (c Config) ForwardAddress() string {
	if c.ForwardAdvertiseAddress != "" {
		return c.ForwardAdvertiseAddress
	}
	return c.ForwardBindAddress
}

// LocalServer returns the local server entry derived from this config
func (c Config) LocalServer() Server {
	return Server{
		ID:             c.LocalID,
		Address:        c.ServerAddress(),
		ForwardAddress: c.ForwardAddress(),
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

func allForwardAddressesSet(srvs []Server) bool {
	for _, srv := range srvs {
		if srv.ForwardAddress == "" {
			return false
		}
	}
	return true
}

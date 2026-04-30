package raft

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/kode4food/timebox"
)

type (
	// Persistence applies Timebox writes through Raft and serves reads from
	// local materialized state
	Persistence struct {
		Config

		db  *kvDB
		fsm *fsm

		node         raft.Node
		raftLog      *raftLog
		transport    *raftTransport
		applyTimeout time.Duration
		flushBatch   func([]decodedEntry, []uint64) error
		lastCommitAt time.Time

		peers      map[uint64]peerInfo
		peerQueues map[uint64]*peerQueue

		bgWG      sync.WaitGroup
		readyCh   chan struct{}
		archiveCh chan struct{}
		readyOnce sync.Once
		stopCh    chan struct{}
		stopOnce  sync.Once
		stopErr   atomic.Value

		publishQ *publishQueue

		pendingMu    sync.Mutex
		pending      map[uint64]proposalState
		nextProposal atomic.Uint64
		appliedIndex atomic.Uint64

		snapMu   sync.Mutex
		snapOut  map[uint64]string
		snapIn   map[uint64]string
		nextSnap atomic.Uint64
	}

	// ServerID identifies one Raft voter
	ServerID string

	// ServerAddress is the advertised Raft transport address for one node
	ServerAddress string

	// State is the current local Raft role
	State string

	peerInfo struct {
		ID       ServerID
		RaftAddr ServerAddress
	}
)

const (
	// StateFollower marks a follower node
	StateFollower State = "follower"

	// StateCandidate marks a candidate or pre-candidate node
	StateCandidate State = "candidate"

	// StateLeader marks the current leader node
	StateLeader State = "leader"

	projectionDirName = "projection"
	projectionDBName  = "badger"
	snapshotDirName   = "snapshots"
)

var _ timebox.Backend = (*Persistence)(nil)
var _ timebox.Archiver = (*Persistence)(nil)

// NewPersistence opens one Raft persistence node
func NewPersistence(cfgs ...Config) (*Persistence, error) {
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	return openPersistence(cfg)
}

// NewStore creates a Store using the current Raft Persistence
func (p *Persistence) NewStore(cfg timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(p, cfg)
}

// Close stops raft and closes local durable state
func (p *Persistence) Close() error {
	var errs []error

	p.stop(nil)
	p.notifyArchive()
	errs = append(errs, p.transport.Close())
	p.bgWG.Wait()
	errs = append(errs, p.raftLog.Close())
	errs = append(errs, p.db.Close())
	return errors.Join(errs...)
}

func openPersistence(cfg Config) (*Persistence, error) {
	db, err := openProjectionDB(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	log, stateExists, err := openRaftLog(cfg)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	tr, err := newRaftTransport(cfg)
	if err != nil {
		_ = log.Close()
		_ = db.Close()
		return nil, err
	}

	p := &Persistence{
		Config:       cfg,
		db:           db,
		raftLog:      log,
		transport:    tr,
		applyTimeout: DefaultApplyTimeout,

		peers:      buildPeerMap(cfg, tr),
		peerQueues: map[uint64]*peerQueue{},

		readyCh:   make(chan struct{}),
		archiveCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),

		pending: map[uint64]proposalState{},
		snapOut: map[uint64]string{},
		snapIn:  map[uint64]string{},
	}
	if cfg.Publisher != nil {
		p.publishQ = newPublishQueue()
	}
	p.fsm = newFSM(db)
	p.flushBatch = p.flushBatchNoPublish
	log.snapshotFn = p.captureSnapshot

	if err := p.restoreMaterializedState(log); err != nil {
		if err := rebuildProjection(p, cfg.DataDir, log); err != nil {
			_ = tr.Close()
			_ = log.Close()
			_ = db.Close()
			return nil, err
		}
		db = p.db
	}
	applied, err := loadLastApplied(db)
	if err != nil {
		_ = tr.Close()
		_ = log.Close()
		_ = db.Close()
		return nil, err
	}
	p.appliedIndex.Store(applied)

	nodeCfg := newRaftNodeConfig(
		nodeID(cfg.LocalID), log, applied,
	)
	switch {
	case stateExists:
		p.node = raft.RestartNode(nodeCfg)
	default:
		p.node = raft.StartNode(nodeCfg, bootstrapPeers(cfg, tr))
	}

	p.startLoops()
	return p, nil
}

func bootstrapPeers(cfg Config, tr *raftTransport) []raft.Peer {
	srvs := cfg.Servers
	if len(srvs) == 0 {
		srv := cfg.LocalServer()
		srv.Address = string(tr.ServerAddress())
		srvs = []Server{srv}
	}

	res := make([]raft.Peer, 0, len(srvs))
	for _, srv := range srvs {
		res = append(res, raft.Peer{
			ID: nodeID(srv.ID),
		})
	}
	return res
}

func buildPeerMap(cfg Config, tr *raftTransport) map[uint64]peerInfo {
	srvs := cfg.Servers
	if len(srvs) == 0 {
		srv := cfg.LocalServer()
		srv.Address = string(tr.ServerAddress())
		srvs = []Server{srv}
	}

	res := make(map[uint64]peerInfo, len(srvs))
	for _, srv := range srvs {
		res[nodeID(srv.ID)] = peerInfo{
			ID:       ServerID(srv.ID),
			RaftAddr: ServerAddress(srv.Address),
		}
	}

	localNID := nodeID(cfg.LocalID)
	local := res[localNID]
	local.ID = ServerID(cfg.LocalID)
	if addr := tr.ServerAddress(); addr != "" {
		local.RaftAddr = addr
	}
	res[localNID] = local
	return res
}

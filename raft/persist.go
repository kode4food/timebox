package raft

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"

	"github.com/kode4food/timebox"
)

type (
	// Persistence applies Timebox writes through Raft and serves reads from
	// local materialized state
	Persistence struct {
		Config

		db  *bbolt.DB
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
		readyOnce sync.Once
		stopCh    chan struct{}
		stopOnce  sync.Once
		stopErr   atomic.Value

		pendingMu    sync.Mutex
		pending      map[uint64]proposalState
		nextProposal atomic.Uint64
		appliedIndex atomic.Uint64
	}

	ServerID      string
	ServerAddress string
	State         string

	peerInfo struct {
		ID       ServerID
		RaftAddr ServerAddress
	}
)

const (
	StateFollower  State = "follower"
	StateCandidate State = "candidate"
	StateLeader    State = "leader"

	projectionDirName = "projection"
	projectionDBName  = "bolt.db"
)

var _ timebox.Backend = (*Persistence)(nil)

// NewStore opens a Timebox store backed by Raft quorum commits and local
// durable state
func NewStore(cfgs ...Config) (*timebox.Store, error) {
	p, err := NewPersistence(cfgs...)
	if err != nil {
		return nil, err
	}

	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	return timebox.NewStore(p, cfg.Timebox)
}

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

		readyCh: make(chan struct{}),
		stopCh:  make(chan struct{}),

		pending: map[uint64]proposalState{},
	}
	p.fsm = newFSM(db, cfg.Timebox.Snapshot.TrimEvents)
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

// Close stops raft and closes local durable state
func (p *Persistence) Close() error {
	var errs []error

	p.stop(nil)
	errs = append(errs, p.transport.Close())
	p.bgWG.Wait()
	errs = append(errs, p.raftLog.Close())
	errs = append(errs, p.db.Close())
	return errors.Join(errs...)
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

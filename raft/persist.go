package raft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kode4food/timebox"
)

type (
	// Persistence applies Timebox writes through Raft and serves reads from
	// local materialized state
	Persistence struct {
		Config

		// Local state
		db  *bbolt.DB
		fsm *fsm

		// Raft runtime
		node         raft.Node
		storage      *raft.MemoryStorage
		raftLog      *raftLog
		transport    *raftTransport
		applyTimeout time.Duration
		flushBatch   func([]decodedEntry, []uint64) error

		// Cluster routing
		peers     map[uint64]peerInfo
		sendChs   map[uint64]chan peerMessage
		compactCh chan struct{}

		// Background loops
		bgWG      sync.WaitGroup
		readyCh   chan struct{}
		readyOnce sync.Once
		stopCh    chan struct{}
		stopOnce  sync.Once
		stopErr   atomic.Value

		// Proposal tracking
		pendingMu    sync.Mutex
		pending      map[uint64]proposalState
		nextProposal atomic.Uint64
		appliedIndex atomic.Uint64
	}

	ServerID      string
	ServerAddress string
	State         string

	// peerInfo holds the addresses for one cluster member
	peerInfo struct {
		ID       ServerID
		RaftAddr ServerAddress
	}
)

const (
	StateFollower  State = "follower"
	StateCandidate State = "candidate"
	StateLeader    State = "leader"
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
	dbPath := filepath.Join(cfg.DataDir, "bbolt.db")
	db, err := openBoltDB(dbPath)
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
		storage:      log.memory,
		raftLog:      log,
		transport:    tr,
		applyTimeout: defaultApplyTimeout,

		peers:     buildPeerMap(cfg, tr),
		sendChs:   map[uint64]chan peerMessage{},
		compactCh: make(chan struct{}, 1),

		readyCh: make(chan struct{}),
		stopCh:  make(chan struct{}),

		pending: map[uint64]proposalState{},
	}
	p.fsm = newFSM(db, cfg.Timebox.Snapshot.TrimEvents)
	p.flushBatch = p.flushBatchNoPublish
	if err := p.restoreSnapshot(log.snapshot); err != nil {
		_ = tr.Close()
		_ = log.Close()
		_ = db.Close()
		return nil, err
	}

	if err := p.restoreMaterializedState(log.CommittedEntries()); err != nil {
		_ = tr.Close()
		_ = log.Close()
		_ = db.Close()
		return nil, err
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
		nodeID(cfg.LocalID), log.memory, applied,
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

// Append submits one aggregate transition through the replicated raft log
func (p *Persistence) Append(req timebox.AppendRequest) error {
	propID := p.newProposalID()
	cmd, err := MakeAppendCommand(propID, &req)
	if err != nil {
		return err
	}
	res, err := p.applyWithTimeout(
		context.Background(),
		cmd,
		propID,
		req.Events,
	)
	if err != nil {
		return err
	}
	return res.Error
}

// LoadEvents returns raw events from the local materialized state
func (p *Persistence) LoadEvents(
	id timebox.AggregateID, fromSeq int64,
) (*timebox.EventsResult, error) {
	var res *timebox.EventsResult

	encodedID := encodeAggregateID(id)
	err := p.db.View(func(tx *bbolt.Tx) error {
		meta, ok, err := loadMetaTx(tx.Bucket(bucketName), encodedID)
		if err != nil {
			return err
		}
		if !ok {
			res = &timebox.EventsResult{
				StartSequence: fromSeq,
				Events:        []*timebox.Event{},
			}
			return nil
		}

		startSeq := max(fromSeq, meta.BaseSequence)

		evs, err := loadEventsTx(tx.Bucket(bucketName), encodedID, startSeq)
		if err != nil {
			return err
		}
		res = &timebox.EventsResult{
			StartSequence: startSeq,
			Events:        evs,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// LoadSnapshot returns the raw snapshot and trailing raw events
func (p *Persistence) LoadSnapshot(
	id timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	var rec *timebox.SnapshotRecord

	encodedID := encodeAggregateID(id)
	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		meta, ok, err := loadMetaTx(b, encodedID)
		if err != nil {
			return err
		}
		if !ok {
			rec = &timebox.SnapshotRecord{}
			return nil
		}

		startSeq := max(meta.SnapshotSequence, meta.BaseSequence)

		evs, err := loadEventsTx(b, encodedID, startSeq)
		if err != nil {
			return err
		}
		rec = &timebox.SnapshotRecord{
			Data:     slices.Clone(b.Get(aggregateSnapshotKey(encodedID))),
			Sequence: meta.SnapshotSequence,
			Events:   evs,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// SaveSnapshot submits snapshot state through the replicated log
func (p *Persistence) SaveSnapshot(
	id timebox.AggregateID, data []byte, sequence int64,
) error {
	propID := p.newProposalID()
	_, err := p.applyWithTimeout(
		context.Background(),
		MakeSnapshotCommand(propID, &SnapshotCommand{
			ID:       id,
			Data:     data,
			Sequence: sequence,
		}),
		propID,
		nil,
	)
	return err
}

// CanSaveSnapshot reports whether this node should originate background
// Timebox snapshot writes
func (p *Persistence) CanSaveSnapshot() bool {
	if len(p.peers) <= 1 {
		return true
	}
	return p.State() == StateLeader
}

// State reports the local Raft node state
func (p *Persistence) State() State {
	switch p.node.Status().RaftState {
	case raft.StateLeader:
		return StateLeader
	case raft.StateCandidate, raft.StatePreCandidate:
		return StateCandidate
	default:
		return StateFollower
	}
}

// Ready reports when the local raft node can serve writes
func (p *Persistence) Ready() <-chan struct{} {
	return p.readyCh
}

// LeaderWithID reports the current leader address and server ID
func (p *Persistence) LeaderWithID() (ServerAddress, ServerID) {
	lead := p.node.Status().Lead
	if lead == 0 {
		return "", ""
	}
	peer := p.peers[lead]
	return peer.RaftAddr, peer.ID
}

// ListAggregates lists aggregates that match the provided prefix
func (p *Persistence) ListAggregates(
	id timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	var ids []timebox.AggregateID

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := AggregateMetaPrefix()
		for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			key := string(k)
			if !strings.HasSuffix(key, metaSuffix) {
				k, _ = c.Next()
				continue
			}
			enc := strings.TrimPrefix(key, aggRootPrefix)
			enc = strings.TrimSuffix(enc, metaSuffix)
			nextID, err := decodeAggregateID(enc)
			if err != nil {
				return err
			}
			if nextID.HasPrefix(id) {
				ids = append(ids, nextID)
			}
			k, _ = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

func (p *Persistence) restoreMaterializedState(ents []raftpb.Entry) error {
	applied, err := loadLastApplied(p.db)
	if err != nil {
		return err
	}

	if err := p.applyCommittedEntries(
		filterEntriesAfter(ents, applied),
	); err != nil {
		return err
	}
	return nil
}

func (p *Persistence) restoreSnapshot(sn raftpb.Snapshot) error {
	if raft.IsEmptySnap(sn) {
		return nil
	}

	applied, err := loadLastApplied(p.db)
	if err != nil {
		return err
	}
	if applied >= sn.Metadata.Index {
		return nil
	}

	src, err := bbolt.Open(snapshotPath(
		p.raftLog.snapshotDir, sn.Metadata.Index,
	), 0o600, &bbolt.Options{
		ReadOnly: true,
		Timeout:  time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	if err := copySnapshot(p.db, src); err != nil {
		return err
	}
	p.appliedIndex.Store(sn.Metadata.Index)
	return os.Remove(snapshotPath(p.raftLog.snapshotDir, sn.Metadata.Index))
}

func openBoltDB(path string) (*bbolt.DB, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout:      time.Second,
		FreelistType: bbolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
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

	// Ensure local node uses actual listen address
	localNID := nodeID(cfg.LocalID)
	local := res[localNID]
	local.ID = ServerID(cfg.LocalID)
	if addr := tr.ServerAddress(); addr != "" {
		local.RaftAddr = addr
	}
	res[localNID] = local
	return res
}

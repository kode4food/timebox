package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	// Persistence applies Timebox writes through etcd raft
	// and serves reads from bbolt
	Persistence struct {
		// Local state
		db  *bbolt.DB
		fsm *fsm

		// Raft runtime
		node         raft.Node
		storage      *raft.MemoryStorage
		raftLog      *raftLog
		transport    *raftTransport
		applyTimeout time.Duration
		appliedIndex uint64

		// Raft compaction
		compactMu      sync.Mutex
		compactMinStep uint64
		compactPending uint64

		// Cluster routing
		peers map[uint64]peerInfo

		// Background loops
		bgWG      sync.WaitGroup
		readyCh   chan struct{}
		readyOnce sync.Once
		stopCh    chan struct{}
		stopOnce  sync.Once

		// Proposal tracking
		pendingMu    sync.Mutex
		pending      map[string]chan proposalResult
		nextProposal atomic.Uint64
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

var _ timebox.Persistence = (*Persistence)(nil)

// NewStore opens a Timebox store backed by etcd raft quorum commits and bbolt
func NewStore(cfgs ...Config) (*timebox.Store, error) {
	p, err := NewPersistence(cfgs...)
	if err != nil {
		return nil, err
	}

	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	return timebox.NewStore(p, cfg.Timebox)
}

// NewPersistence opens one etcd raft + bbolt persistence node
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
		db:             db,
		storage:        log.memory,
		raftLog:        log,
		transport:      tr,
		applyTimeout:   defaultApplyTimeout,
		compactMinStep: compactMinStep(),

		peers: buildPeerMap(cfg, tr),

		readyCh: make(chan struct{}),
		stopCh:  make(chan struct{}),

		pending: map[string]chan proposalResult{},
	}
	p.fsm = newFSM(db, cfg.Timebox.Snapshot.TrimEvents)

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
	p.appliedIndex = applied

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

	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	p.node.Stop()
	errs = append(errs, p.transport.Close())
	p.bgWG.Wait()
	errs = append(errs, p.raftLog.Close())
	errs = append(errs, p.db.Close())
	return errors.Join(errs...)
}

// Append submits one aggregate transition through the replicated raft log
func (p *Persistence) Append(
	req timebox.AppendRequest,
) (*timebox.AppendResult, error) {
	res, err := p.applyWithTimeout(
		context.Background(), command{
			Type:   commandAppend,
			Append: &appendCommand{Request: req},
		},
	)
	if err != nil {
		return nil, err
	}
	return res.Conflict, nil
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
				Events:        []json.RawMessage{},
			}
			return nil
		}

		startSeq := max(fromSeq, meta.BaseSequence)

		evs, err := loadRawEventsTx(tx.Bucket(bucketName), encodedID, startSeq)
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

		evs, err := loadRawEventsTx(b, encodedID, startSeq)
		if err != nil {
			return err
		}
		rec = &timebox.SnapshotRecord{
			Data:     cloneBytes(b.Get(aggregateSnapshotKey(encodedID))),
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
	ctx context.Context, id timebox.AggregateID,
	data []byte, sequence int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := p.applyWithTimeout(ctx, command{
		Type: commandSnapshot,
		Snapshot: &snapshotCommand{
			ID:       id,
			Data:     data,
			Sequence: sequence,
		},
	})
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

// State reports the local etcd raft node state
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
		pfx := aggregateMetaPrefix()
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

// GetAggregateStatus returns the current derived status for one aggregate
func (p *Persistence) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	var status string
	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		meta, ok, err := loadMetaTx(b, encodeAggregateID(id))
		if err != nil {
			return err
		}
		if ok {
			status = meta.Status
		}
		return nil
	})
	return status, err
}

// ListAggregatesByStatus lists aggregates currently indexed by status
func (p *Persistence) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	var res []timebox.StatusEntry

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := statusIndexPrefix(status)
		for k, v := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			parts := strings.Split(string(k), "/")
			id, err := decodeAggregateID(parts[len(parts)-1])
			if err != nil {
				return err
			}
			ts, err := decodeInt64(v)
			if err != nil {
				return err
			}
			res = append(res, timebox.StatusEntry{
				ID:        id,
				Timestamp: time.UnixMilli(ts).UTC(),
			})
			k, v = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(res, func(i, j int) bool {
		return res[i].Timestamp.Before(res[j].Timestamp)
	})
	return res, nil
}

// ListAggregatesByLabel lists aggregates currently indexed by label/value
func (p *Persistence) ListAggregatesByLabel(
	label, value string,
) ([]timebox.AggregateID, error) {
	var ids []timebox.AggregateID

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := labelIndexPrefix(label, value)
		for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			parts := strings.Split(string(k), "/")
			id, err := decodeAggregateID(parts[len(parts)-1])
			if err != nil {
				return err
			}
			ids = append(ids, id)
			k, _ = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// ListLabelValues lists the current indexed values for one label
func (p *Persistence) ListLabelValues(label string) ([]string, error) {
	var vals []string

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := labelValuesPrefix(label)
		for k, _ := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			parts := strings.Split(string(k), "/")
			value, err := decodeKeyPart(parts[len(parts)-1])
			if err != nil {
				return err
			}
			vals = append(vals, value)
			k, _ = c.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Strings(vals)
	return vals, nil
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

func (p *Persistence) applyWithTimeout(
	ctx context.Context, cmd command,
) (*applyResult, error) {
	cmd.ProposalID = p.newProposalID()
	data, err := encodeCommand(cmd)
	if err != nil {
		return nil, err
	}
	timeout, err := p.commandTimeout(ctx)
	if err != nil {
		return nil, err
	}
	proposeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return p.propose(proposeCtx, data, cmd.ProposalID)
}

func (p *Persistence) commandTimeout(
	ctx context.Context,
) (time.Duration, error) {
	timeout := p.applyTimeout
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); rem < timeout {
			timeout = rem
		}
	}
	if timeout <= 0 {
		return 0, ctx.Err()
	}
	return timeout, nil
}

func (p *Persistence) markReadyIfPossible() {
	if p.State() == StateLeader {
		p.markReady()
		return
	}
	addr, _ := p.LeaderWithID()
	if addr != "" {
		p.markReady()
	}
}

func (p *Persistence) markReady() {
	p.readyOnce.Do(func() {
		close(p.readyCh)
	})
}

func (p *Persistence) markAppliedEntry(index uint64) error {
	err := p.db.Update(func(tx *bbolt.Tx) error {
		return markApplied(tx.Bucket(bucketName), index)
	})
	if err != nil {
		return err
	}
	p.appliedIndex = index
	return nil
}

func (p *Persistence) newProposalID() string {
	return encodeSequence(int64(p.nextProposal.Add(1)))
}

func compactMinStep() uint64 {
	if value := os.Getenv(testCompactStepEnv); value != "" {
		if step, err := strconv.Atoi(value); err == nil && step > 0 {
			return uint64(step)
		}
	}
	return defaultCompactMinStep
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

func buildPeerMap(
	cfg Config, tr *raftTransport,
) map[uint64]peerInfo {
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

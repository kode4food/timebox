package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
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
		db         *bbolt.DB
		fsm        *fsm
		statusMu   sync.RWMutex
		statusByID map[string]string

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
		forwarder         *forwardServer
		forwardPool       *forwardPool
		forwardAddr       ServerAddress
		forwardByID       map[ServerID]ServerAddress
		forwardByRaftAddr map[ServerAddress]ServerAddress
		addrByNodeID      map[uint64]ServerAddress
		idByNodeID        map[uint64]ServerID
		forwardMu         sync.Mutex

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
)

const (
	StateFollower  State = "follower"
	StateCandidate State = "candidate"
	StateLeader    State = "leader"
	StateShutdown  State = "shutdown"

	forwardRetryDelay  = 25 * time.Millisecond
	forwardMaxAttempts = 3
)

var (
	ErrNotLeader         = errors.New("not leader")
	ErrBootstrapRequired = errors.New(
		"raft bootstrap must be enabled when no local state exists",
	)
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
	forwarder, err := newForwardServer(cfg)
	if err != nil {
		_ = tr.Close()
		_ = log.Close()
		_ = db.Close()
		return nil, err
	}

	p := &Persistence{
		db:         db,
		statusByID: map[string]string{},

		storage:        log.memory,
		raftLog:        log,
		transport:      tr,
		applyTimeout:   cfg.ApplyTimeout,
		compactMinStep: uint64(cfg.CompactMinStep),

		forwarder:         forwarder,
		forwardByID:       forwardAddressesByID(cfg, forwarder),
		forwardByRaftAddr: forwardAddressesByRaftAddr(cfg, forwarder, tr),
		addrByNodeID:      serverAddressesByNodeID(cfg, tr),
		idByNodeID:        serverIDsByNodeID(cfg, tr),

		readyCh: make(chan struct{}),
		stopCh:  make(chan struct{}),

		pending: map[string]chan proposalResult{},
	}
	p.fsm = newFSM(db, cfg.Timebox.Snapshot.TrimEvents, p.setAggregateStatus)

	if err := p.restoreMaterializedState(log.CommittedEntries()); err != nil {
		_ = forwarder.Close()
		_ = tr.Close()
		_ = log.Close()
		_ = db.Close()
		return nil, err
	}
	applied, err := loadLastApplied(db)
	if err != nil {
		_ = forwarder.Close()
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
	if cfg.ForwardBindAddress != "" {
		p.serveForwarding()
	}
	return p, nil
}

// Close stops raft and closes local durable state
func (p *Persistence) Close() error {
	var errs []error

	p.stopOnce.Do(func() {
		if p.stopCh != nil {
			close(p.stopCh)
		}
	})
	if p.node != nil {
		p.node.Stop()
	}
	if p.transport != nil {
		errs = append(errs, p.transport.Close())
	}
	if p.forwarder != nil {
		errs = append(errs, p.forwarder.Close())
	}
	if pool := p.takeForwardPool(); pool != nil {
		errs = append(errs, pool.Close())
	}
	p.bgWG.Wait()
	if p.raftLog != nil {
		errs = append(errs, p.raftLog.Close())
	}
	if p.db != nil {
		errs = append(errs, p.db.Close())
	}
	return errors.Join(errs...)
}

// Append submits one aggregate transition through the replicated raft log
func (p *Persistence) Append(
	req timebox.AppendRequest,
) (*timebox.AppendResult, error) {
	res, err := p.executeCommand(context.Background(), command{
		Type:       commandAppend,
		ProposalID: p.newProposalID(),
		Append:     &appendCommand{Request: req},
	})
	if err != nil {
		return nil, err
	}
	p.recordAppendStatus(req, res)
	return res.Conflict, nil
}

// LoadEvents returns raw events from the local materialized state
func (p *Persistence) LoadEvents(
	id timebox.AggregateID, fromSeq int64,
) (*timebox.EventsResult, error) {
	var res *timebox.EventsResult

	err := p.db.View(func(tx *bbolt.Tx) error {
		meta, ok, err := loadMetaTx(tx.Bucket(bucketName), id)
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

		evs, err := loadRawEventsTx(tx.Bucket(bucketName), id, startSeq)
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

	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		meta, ok, err := loadMetaTx(b, id)
		if err != nil {
			return err
		}
		if !ok {
			rec = &timebox.SnapshotRecord{}
			return nil
		}

		startSeq := max(meta.SnapshotSequence, meta.BaseSequence)

		evs, err := loadRawEventsTx(b, id, startSeq)
		if err != nil {
			return err
		}
		rec = &timebox.SnapshotRecord{
			Data: json.RawMessage(
				cloneBytes(b.Get(aggregateSnapshotKey(id))),
			),
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
	ctx context.Context, id timebox.AggregateID, data []byte, sequence int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := p.executeCommand(ctx, command{
		Type:       commandSnapshot,
		ProposalID: p.newProposalID(),
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
	if len(p.addrByNodeID) <= 1 {
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

// Ready reports when the local raft node can serve writes directly
// or forward them
func (p *Persistence) Ready() <-chan struct{} {
	return p.readyCh
}

// LeaderWithID reports the current leader address and server ID
func (p *Persistence) LeaderWithID() (ServerAddress, ServerID) {
	lead := p.node.Status().Lead
	if lead == 0 {
		return "", ""
	}
	return p.addrByNodeID[lead], p.idByNodeID[lead]
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

	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Join(":") < ids[j].Join(":")
	})
	return ids, nil
}

// GetAggregateStatus returns the current derived status for one aggregate
func (p *Persistence) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	if status, ok := p.aggregateStatus(id); ok {
		return status, nil
	}
	return "", nil
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
		return res[i].ID.Join(":") < res[j].ID.Join(":")
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

	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Join(":") < ids[j].Join(":")
	})
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
	return p.reloadStatusCache()
}

func (p *Persistence) aggregateStatus(
	id timebox.AggregateID,
) (string, bool) {
	key := id.Join(":")
	p.statusMu.RLock()
	status, ok := p.statusByID[key]
	p.statusMu.RUnlock()
	return status, ok
}

func (p *Persistence) setAggregateStatus(
	id timebox.AggregateID, status string,
) {
	key := id.Join(":")
	p.statusMu.Lock()
	if status == "" {
		delete(p.statusByID, key)
	} else {
		p.statusByID[key] = status
	}
	p.statusMu.Unlock()
}

func (p *Persistence) recordAppendStatus(
	req timebox.AppendRequest, res *applyResult,
) {
	if req.Status == nil {
		return
	}
	if res != nil && res.Conflict != nil {
		return
	}
	p.setAggregateStatus(req.ID, *req.Status)
}

func (p *Persistence) reloadStatusCache() error {
	next := map[string]string{}
	err := p.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		pfx := aggregateMetaPrefix()
		for k, v := c.Seek(pfx); k != nil && bytes.HasPrefix(k, pfx); {
			meta, err := unmarshalMeta(v)
			if err != nil {
				return err
			}
			if meta.Status != "" {
				key := string(bytes.TrimSuffix(
					bytes.TrimPrefix(k, []byte(aggRootPrefix)),
					[]byte(metaSuffix),
				))
				id, err := decodeAggregateID(key)
				if err != nil {
					return err
				}
				next[id.Join(":")] = meta.Status
			}
			k, v = c.Next()
		}
		return nil
	})
	if err != nil {
		return err
	}

	p.statusMu.Lock()
	p.statusByID = next
	p.statusMu.Unlock()
	return nil
}

func (p *Persistence) executeCommand(
	ctx context.Context, cmd command,
) (*applyResult, error) {
	for attempt := range forwardMaxAttempts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		res, err := p.applyAsLeader(ctx, cmd)
		switch {
		case err == nil:
			return normalizeApplyResult(res), nil
		case !errors.Is(err, ErrNotLeader):
			return nil, err
		}

		res, err = p.forwardCommand(ctx, cmd)
		switch {
		case err == nil:
			return normalizeApplyResult(res), nil
		case !errors.Is(err, ErrNotLeader):
			return nil, err
		}

		if attempt == forwardMaxAttempts-1 {
			break
		}

		timer := time.NewTimer(forwardRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return nil, ErrNotLeader
}

func (p *Persistence) applyAsLeader(
	ctx context.Context, cmd command,
) (*applyResult, error) {
	if p.State() != StateLeader {
		return nil, ErrNotLeader
	}
	return p.applyWithTimeout(ctx, cmd)
}

func (p *Persistence) applyEncodedAsLeader(
	ctx context.Context, data []byte,
) (*applyResult, error) {
	if p.State() != StateLeader {
		return nil, ErrNotLeader
	}
	return p.applyEncoded(ctx, data)
}

func (p *Persistence) applyWithTimeout(
	ctx context.Context, cmd command,
) (*applyResult, error) {
	if cmd.ProposalID == "" {
		cmd.ProposalID = p.newProposalID()
	}
	data, err := encodeCommand(cmd)
	if err != nil {
		return nil, err
	}
	return p.applyEncoded(ctx, data)
}

func (p *Persistence) applyEncoded(
	ctx context.Context, data []byte,
) (*applyResult, error) {
	timeout, err := p.commandTimeout(ctx)
	if err != nil {
		return nil, err
	}
	cmd, err := decodeCommand(data)
	if err != nil {
		return nil, err
	}
	if cmd.ProposalID == "" {
		cmd.ProposalID = p.newProposalID()
		data, err = encodeCommand(*cmd)
		if err != nil {
			return nil, err
		}
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
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		timeout = p.applyTimeout
	}
	return timeout, nil
}

func (p *Persistence) forwardPoolFor(
	addr ServerAddress,
) (*forwardPool, error) {
	p.forwardMu.Lock()
	if p.forwardPool != nil && p.forwardAddr == addr {
		pool := p.forwardPool
		p.forwardMu.Unlock()
		return pool, nil
	}

	old := p.forwardPool
	pool := newForwardPool(p.forwarder, addr)
	p.forwardPool = pool
	p.forwardAddr = addr
	p.forwardMu.Unlock()

	if old != nil {
		_ = old.Close()
	}
	return pool, nil
}

func (p *Persistence) takeForwardPool() *forwardPool {
	p.forwardMu.Lock()
	defer p.forwardMu.Unlock()

	pool := p.forwardPool
	p.forwardPool = nil
	p.forwardAddr = ""
	return pool
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

func (p *Persistence) updateLeader(_ uint64) {}

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

func serverAddressesByNodeID(
	cfg Config, tr *raftTransport,
) map[uint64]ServerAddress {
	srvs := cfg.Servers
	if len(srvs) == 0 {
		srvs = []Server{cfg.LocalServer()}
		srvs[0].Address = string(tr.ServerAddress())
	}

	res := make(map[uint64]ServerAddress, len(srvs))
	for _, srv := range srvs {
		res[nodeID(srv.ID)] = ServerAddress(srv.Address)
	}
	if addr := tr.ServerAddress(); addr != "" {
		res[nodeID(cfg.LocalID)] = addr
	}
	return res
}

func serverIDsByNodeID(cfg Config, tr *raftTransport) map[uint64]ServerID {
	srvs := cfg.Servers
	if len(srvs) == 0 {
		srvs = []Server{cfg.LocalServer()}
		srvs[0].Address = string(tr.ServerAddress())
	}

	res := make(map[uint64]ServerID, len(srvs))
	for _, srv := range srvs {
		res[nodeID(srv.ID)] = ServerID(srv.ID)
	}
	res[nodeID(cfg.LocalID)] = ServerID(cfg.LocalID)
	return res
}

func forwardAddressesByID(
	cfg Config, forwarder *forwardServer,
) map[ServerID]ServerAddress {
	if len(cfg.Servers) == 0 && forwarder == nil {
		return nil
	}

	res := make(map[ServerID]ServerAddress, len(cfg.Servers))
	for _, srv := range cfg.Servers {
		if srv.ForwardAddress == "" {
			continue
		}
		res[ServerID(srv.ID)] = ServerAddress(srv.ForwardAddress)
	}

	if forwarder != nil {
		addr := forwarder.ServerAddress()
		if addr != "" {
			res[ServerID(cfg.LocalID)] = addr
		}
	}
	return res
}

func forwardAddressesByRaftAddr(
	cfg Config, forwarder *forwardServer, tr *raftTransport,
) map[ServerAddress]ServerAddress {
	if len(cfg.Servers) == 0 && forwarder == nil {
		return nil
	}

	res := make(map[ServerAddress]ServerAddress, len(cfg.Servers))
	for _, srv := range cfg.Servers {
		if srv.Address == "" || srv.ForwardAddress == "" {
			continue
		}
		res[ServerAddress(srv.Address)] = ServerAddress(srv.ForwardAddress)
	}

	if forwarder != nil {
		raftAddr := tr.ServerAddress()
		forwardAddr := forwarder.ServerAddress()
		if raftAddr != "" && forwardAddr != "" {
			res[raftAddr] = forwardAddr
		}
	}
	return res
}

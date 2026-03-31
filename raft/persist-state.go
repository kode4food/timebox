package raft

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const snapshotRefSize = 8

func openProjectionDB(dataDir string) (*kvDB, error) {
	return openKVDB(kvPath(dataDir, projectionDirName, projectionDBName))
}

// State returns the current local Raft role
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

// Ready closes once the node is ready to serve leader-directed traffic
func (p *Persistence) Ready() <-chan struct{} {
	return p.readyCh
}

// LeaderWithID returns the current leader address and server ID
func (p *Persistence) LeaderWithID() (ServerAddress, ServerID) {
	lead := p.node.Status().Lead
	if lead == 0 {
		return "", ""
	}
	peer := p.peers[lead]
	return peer.RaftAddr, peer.ID
}

func (p *Persistence) compactBound() uint64 {
	applied := p.appliedIndex.Load()
	st := p.node.Status()
	if st.RaftState != raft.StateLeader {
		return applied
	}
	bound := applied
	for _, pr := range st.Progress {
		if pr.Match > 0 && pr.Match < bound {
			bound = pr.Match
		}
	}
	return bound
}

func (p *Persistence) captureSnapshot() ([]byte, uint64, error) {
	var applied uint64
	err := p.db.View(func(tx *kvTx) error {
		var err error
		applied, err = loadLastAppliedTx(tx.Bucket(bucketName))
		return err
	})
	if err != nil {
		return nil, 0, err
	}

	ref, path, err := p.newSnapshotRef()
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		_ = f.Close()
	}()
	if err := backupKVDBTo(p.db, f); err != nil {
		_ = os.Remove(path)
		return nil, 0, err
	}
	if err := f.Sync(); err != nil {
		_ = os.Remove(path)
		return nil, 0, err
	}

	p.storeOutgoingSnapshot(ref, path)
	return encodeSnapshotRef(ref), applied, nil
}

func (p *Persistence) applySnapshot(snap raftpb.Snapshot) error {
	_ = p.db.Close()

	path := filepath.Join(p.DataDir, projectionDirName, projectionDBName)
	if ref, ok := decodeSnapshotRef(snap.Data); ok {
		src, ok := p.takeIncomingSnapshot(ref)
		if !ok {
			return raft.ErrSnapshotTemporarilyUnavailable
		}
		defer func() {
			_ = os.Remove(src)
		}()

		f, err := os.Open(src)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()

		db, err := replaceKVDBFrom(path, f)
		if err != nil {
			return err
		}
		p.db = db
		p.fsm = newFSM(db)
		p.appliedIndex.Store(snap.Metadata.Index)
		return p.raftLog.ApplySnapshot(snap.Metadata)
	}

	db, err := replaceKVDBFrom(path, bytes.NewReader(snap.Data))
	if err != nil {
		return err
	}
	p.db = db
	p.fsm = newFSM(db)
	p.appliedIndex.Store(snap.Metadata.Index)
	return p.raftLog.ApplySnapshot(snap.Metadata)
}

func (p *Persistence) restoreMaterializedState(log *raftLog) error {
	applied, err := loadLastApplied(p.db)
	if err != nil {
		return err
	}
	return log.ReplayCommitted(applied, p.applyStartupEntries)
}

func (p *Persistence) newSnapshotRef() (uint64, string, error) {
	dir := filepath.Join(p.DataDir, snapshotDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, "", err
	}
	ref := p.nextSnap.Add(1)
	return ref, filepath.Join(dir, snapshotFileName(ref)), nil
}

func (p *Persistence) storeOutgoingSnapshot(ref uint64, path string) {
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	p.snapOut[ref] = path
}

func (p *Persistence) outgoingSnapshot(ref uint64) (string, bool) {
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	path, ok := p.snapOut[ref]
	return path, ok
}

func (p *Persistence) releaseOutgoingSnapshot(ref uint64) {
	p.snapMu.Lock()
	path, ok := p.snapOut[ref]
	if ok {
		delete(p.snapOut, ref)
	}
	p.snapMu.Unlock()
	if ok {
		_ = os.Remove(path)
	}
}

func (p *Persistence) storeIncomingSnapshot(ref uint64, path string) {
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	if prev, ok := p.snapIn[ref]; ok {
		_ = os.Remove(prev)
	}
	p.snapIn[ref] = path
}

func (p *Persistence) takeIncomingSnapshot(ref uint64) (string, bool) {
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	path, ok := p.snapIn[ref]
	if ok {
		delete(p.snapIn, ref)
	}
	return path, ok
}

func (p *Persistence) writeSnapshotStream(w io.Writer, ref uint64) error {
	path, ok := p.outgoingSnapshot(ref)
	if !ok {
		return raft.ErrSnapshotTemporarilyUnavailable
	}
	return writeSnapshotFile(w, path)
}

func (p *Persistence) readSnapshotStream(
	r io.Reader, ref uint64, size uint64,
) error {
	_, path, err := p.newSnapshotRef()
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	if _, err := io.CopyN(f, r, int64(size)); err != nil {
		_ = os.Remove(path)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = os.Remove(path)
		return err
	}
	p.storeIncomingSnapshot(ref, path)
	return nil
}

func rebuildProjection(p *Persistence, dataDir string, log *raftLog) error {
	_ = p.db.Close()
	path := kvPath(dataDir, projectionDirName, projectionDBName)
	if err := removeKVPath(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	db, err := openProjectionDB(dataDir)
	if err != nil {
		return err
	}
	p.db = db
	p.fsm = newFSM(db)
	return p.restoreMaterializedState(log)
}

func encodeSnapshotRef(ref uint64) []byte {
	var b [snapshotRefSize]byte
	binary.BigEndian.PutUint64(b[:], ref)
	return b[:]
}

func decodeSnapshotRef(data []byte) (uint64, bool) {
	if len(data) != snapshotRefSize {
		return 0, false
	}
	return binary.BigEndian.Uint64(data), true
}

func snapshotFileName(ref uint64) string {
	var b [snapshotRefSize]byte
	binary.BigEndian.PutUint64(b[:], ref)
	return "snap-" + hexString(b[:]) + ".bak"
}

func hexString(data []byte) string {
	const hex = "0123456789abcdef"
	buf := make([]byte, len(data)*2)
	for i, b := range data {
		buf[i*2] = hex[b>>4]
		buf[i*2+1] = hex[b&0x0f]
	}
	return string(buf)
}

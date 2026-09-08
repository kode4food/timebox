package raft

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	bin "github.com/kode4food/timebox/internal/binary"
)

const snapshotRefSize = 8

func openProjectionDB(dataDir string) (*kvDB, error) {
	return openKVDB(kvPath(dataDir, projectionDirName, projectionDBName))
}

// State returns the current local Raft role
func (b *Backend) State() State {
	switch b.node.Status().RaftState {
	case raft.StateLeader:
		return StateLeader
	case raft.StateCandidate, raft.StatePreCandidate:
		return StateCandidate
	default:
		return StateFollower
	}
}

// Ready closes once the node is ready to serve leader-directed traffic
func (b *Backend) Ready() <-chan struct{} {
	return b.readyCh
}

// LeaderWithID returns the current leader address and server ID
func (b *Backend) LeaderWithID() (ServerAddress, ServerID) {
	lead := b.node.Status().Lead
	if lead == 0 {
		return "", ""
	}
	peer := b.peers[lead]
	return peer.RaftAddr, peer.ID
}

func (b *Backend) compactBound() uint64 {
	applied := b.appliedIndex.Load()
	st := b.node.Status()
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

func (b *Backend) captureSnapshot() ([]byte, uint64, error) {
	var applied uint64
	err := b.db.View(func(tx *kvTx) error {
		var err error
		applied, err = loadLastAppliedTx(tx.Bucket(bucketName))
		return err
	})
	if err != nil {
		return nil, 0, err
	}

	ref, path, err := b.newSnapshotRef()
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
	if err := backupKVDBTo(b.db, f); err != nil {
		_ = os.Remove(path)
		return nil, 0, err
	}
	if err := f.Sync(); err != nil {
		_ = os.Remove(path)
		return nil, 0, err
	}

	b.storeOutgoingSnapshot(ref, path)
	return encodeSnapshotRef(ref), applied, nil
}

func (b *Backend) applySnapshot(snap *raftpb.Snapshot) error {
	_ = b.db.Close()

	data := snap.GetData()
	meta := snap.GetMetadata()
	path := filepath.Join(b.cfg.DataDir, projectionDirName, projectionDBName)
	if ref, ok := decodeSnapshotRef(data); ok {
		src, ok := b.takeIncomingSnapshot(ref)
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
		b.db = db
		b.fsm = newFSM(db)
		b.appliedIndex.Store(meta.GetIndex())
		return b.raftLog.ApplySnapshot(meta)
	}

	db, err := replaceKVDBFrom(path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	b.db = db
	b.fsm = newFSM(db)
	b.appliedIndex.Store(meta.GetIndex())
	return b.raftLog.ApplySnapshot(meta)
}

func (b *Backend) restoreMaterializedState(
	log *raftLog, fastForward bool,
) error {
	applied, err := loadLastApplied(b.db)
	if err != nil {
		return err
	}
	if applied > log.CommitIndex() {
		return bin.ErrCorruptState
	}
	if compacted := log.Compacted(); applied < compacted {
		if !fastForward {
			return raft.ErrCompacted
		}
		if err := b.markAppliedEntry(compacted); err != nil {
			return err
		}
		applied = compacted
	}
	return log.ReplayCommitted(applied, b.applyStartupEntries)
}

func (b *Backend) newSnapshotRef() (uint64, string, error) {
	dir := filepath.Join(b.cfg.DataDir, snapshotDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, "", err
	}
	ref := b.nextSnap.Add(1)
	return ref, filepath.Join(dir, snapshotFileName(ref)), nil
}

func (b *Backend) storeOutgoingSnapshot(ref uint64, path string) {
	b.snapMu.Lock()
	defer b.snapMu.Unlock()
	b.snapOut[ref] = path
}

func (b *Backend) outgoingSnapshot(ref uint64) (string, bool) {
	b.snapMu.Lock()
	defer b.snapMu.Unlock()
	path, ok := b.snapOut[ref]
	return path, ok
}

func (b *Backend) releaseOutgoingSnapshot(ref uint64) {
	b.snapMu.Lock()
	path, ok := b.snapOut[ref]
	if ok {
		delete(b.snapOut, ref)
	}
	b.snapMu.Unlock()
	if ok {
		_ = os.Remove(path)
	}
}

func (b *Backend) storeIncomingSnapshot(ref uint64, path string) {
	b.snapMu.Lock()
	defer b.snapMu.Unlock()
	if prev, ok := b.snapIn[ref]; ok {
		_ = os.Remove(prev)
	}
	b.snapIn[ref] = path
}

func (b *Backend) takeIncomingSnapshot(ref uint64) (string, bool) {
	b.snapMu.Lock()
	defer b.snapMu.Unlock()
	path, ok := b.snapIn[ref]
	if ok {
		delete(b.snapIn, ref)
	}
	return path, ok
}

func (b *Backend) writeSnapshotStream(w io.Writer, ref uint64) error {
	path, ok := b.outgoingSnapshot(ref)
	if !ok {
		return raft.ErrSnapshotTemporarilyUnavailable
	}
	return writeSnapshotFile(w, path)
}

func (b *Backend) readSnapshotStream(
	r io.Reader, ref uint64, size uint64,
) error {
	_, path, err := b.newSnapshotRef()
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
	b.storeIncomingSnapshot(ref, path)
	return nil
}

func rebuildProjection(b *Backend, dataDir string, log *raftLog) error {
	_ = b.db.Close()
	path := kvPath(dataDir, projectionDirName, projectionDBName)
	if err := removeKVPath(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	db, err := openProjectionDB(dataDir)
	if err != nil {
		return err
	}
	b.db = db
	b.fsm = newFSM(db)
	return b.restoreMaterializedState(log, false)
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
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

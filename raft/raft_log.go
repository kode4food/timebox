package raft

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type raftLog struct {
	mu             sync.RWMutex
	log            storage.Storage
	memory         *raft.MemoryStorage
	hardState      raftpb.HardState
	entries        []raftpb.Entry
	snapshot       raftpb.Snapshot
	snapshotDir    string
	snapshotRetain int
}

const (
	raftWALDirName  = "raft-wal"
	raftSnapDirName = "raft-snap"
	raftSnapSuffix  = ".snap"
)

func (r *raftLog) Close() error {
	return r.log.Close()
}

func (r *raftLog) Save(rd raft.Ready) error {
	if err := r.log.Save(rd.HardState, rd.Entries); err != nil {
		return err
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		r.mu.Lock()
		r.hardState = rd.HardState
		r.mu.Unlock()
	}
	return nil
}

func (r *raftLog) AppendEntries(ents []raftpb.Entry) error {
	if err := r.memory.Append(ents); err != nil {
		return err
	}
	r.mu.Lock()
	r.entries = mergeEntries(r.entries, ents)
	r.mu.Unlock()
	return nil
}

func (r *raftLog) Compact(
	snapshotIndex, compactIndex uint64, cs raftpb.ConfState,
) error {
	if snapshotIndex == 0 || snapshotIndex <= r.snapshotIndex() {
		return nil
	}

	sn, err := r.memory.CreateSnapshot(snapshotIndex, &cs, nil)
	switch {
	case err == nil:
	case errors.Is(err, raft.ErrSnapOutOfDate):
		return nil
	default:
		return err
	}

	if err := r.log.SaveSnap(sn); err != nil {
		return err
	}
	if err := r.memory.Compact(compactIndex); err != nil &&
		!errors.Is(err, raft.ErrCompacted) {
		return err
	}
	if err := r.log.Sync(); err != nil {
		return err
	}
	if err := r.log.Release(sn); err != nil {
		return err
	}

	r.mu.Lock()
	r.snapshot = sn
	r.entries = filterEntriesAfter(r.entries, compactIndex)
	r.mu.Unlock()
	return r.pruneSnapshots()
}

func (r *raftLog) ApplySnapshot(sn raftpb.Snapshot) error {
	if raft.IsEmptySnap(sn) {
		return nil
	}
	if err := r.log.SaveSnap(sn); err != nil {
		return err
	}
	switch err := r.memory.ApplySnapshot(sn); {
	case err == nil:
	case errors.Is(err, raft.ErrSnapOutOfDate):
		return nil
	default:
		return err
	}
	if err := r.log.Sync(); err != nil {
		return err
	}
	if err := r.log.Release(sn); err != nil {
		return err
	}

	r.mu.Lock()
	r.snapshot = sn
	r.entries = filterEntriesAfter(r.entries, sn.Metadata.Index)
	r.mu.Unlock()
	return r.pruneSnapshots()
}

func (r *raftLog) CommittedEntries() []raftpb.Entry {
	r.mu.RLock()
	commit := r.hardState.Commit
	ents := append([]raftpb.Entry(nil), r.entries...)
	r.mu.RUnlock()
	if commit == 0 || len(ents) == 0 {
		return nil
	}

	n := 0
	for _, ent := range ents {
		if ent.Index > commit {
			break
		}
		n++
	}
	return append([]raftpb.Entry(nil), ents[:n]...)
}

func (r *raftLog) snapshotIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshot.Metadata.Index
}

func primeConfState(
	lg *zap.Logger, ms *raft.MemoryStorage, ents []raftpb.Entry,
) error {
	sn := raftpb.Snapshot{}
	ids := storage.GetEffectiveNodeIDsFromWALEntries(lg, &sn, ents)
	if len(ids) == 0 {
		return nil
	}

	lastIdx, err := ms.LastIndex()
	if err != nil || lastIdx == 0 {
		return err
	}

	cs := raftpb.ConfState{Voters: ids}
	_, err = ms.CreateSnapshot(lastIdx, &cs, nil)
	if errors.Is(err, raft.ErrSnapOutOfDate) {
		return nil
	}
	return err
}

func openRaftLog(cfg Config) (*raftLog, bool, error) {
	lg := newEtcdLogger()
	walDir := filepath.Join(cfg.DataDir, raftWALDirName)
	snapDir := filepath.Join(cfg.DataDir, raftSnapDirName)
	haveWAL := wal.Exist(walDir)

	ss, err := openSnapshotter(lg, snapDir)
	if err != nil {
		return nil, false, err
	}
	w, snapData, hs, ents, err := openRaftWAL(lg, walDir, ss, haveWAL)
	if err != nil {
		return nil, false, err
	}

	st := storage.NewStorage(lg, w, ss)
	ms := raft.NewMemoryStorage()

	if !raft.IsEmptySnap(snapData) {
		if err := ms.ApplySnapshot(snapData); err != nil {
			_ = st.Close()
			return nil, false, err
		}
	}
	if !raft.IsEmptyHardState(hs) {
		if err := ms.SetHardState(hs); err != nil {
			_ = st.Close()
			return nil, false, err
		}
	}
	if len(ents) != 0 {
		if err := ms.Append(ents); err != nil {
			_ = st.Close()
			return nil, false, err
		}
	}
	if raft.IsEmptySnap(snapData) {
		if err := primeConfState(lg, ms, ents); err != nil {
			_ = st.Close()
			return nil, false, err
		}
	}

	res := &raftLog{
		log:            st,
		memory:         ms,
		hardState:      hs,
		entries:        filterEntriesAfter(ents, snapData.Metadata.Index),
		snapshot:       snapData,
		snapshotDir:    snapDir,
		snapshotRetain: defaultSnapshotRetain,
	}
	stateExists := haveWAL && (!raft.IsEmptyHardState(hs) ||
		len(ents) != 0 || !raft.IsEmptySnap(snapData))
	return res, stateExists, nil
}

func mergeEntries(base, ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return base
	}
	if len(base) == 0 {
		return append([]raftpb.Entry(nil), ents...)
	}

	first := ents[0].Index
	keep := len(base)
	for i, ent := range base {
		if ent.Index >= first {
			keep = i
			break
		}
	}

	res := append([]raftpb.Entry(nil), base[:keep]...)
	return append(res, ents...)
}

func filterEntriesAfter(ents []raftpb.Entry, index uint64) []raftpb.Entry {
	if len(ents) == 0 {
		return nil
	}

	var res []raftpb.Entry
	for _, ent := range ents {
		if ent.Index > index {
			res = append(res, ent)
		}
	}
	return res
}

func openRaftWAL(
	lg *zap.Logger, walDir string, ss *snap.Snapshotter, haveWAL bool,
) (*wal.WAL, raftpb.Snapshot, raftpb.HardState, []raftpb.Entry, error) {
	if !haveWAL {
		w, err := wal.Create(lg, walDir, nil)
		if err != nil {
			return nil, raftpb.Snapshot{}, raftpb.HardState{}, nil, err
		}
		return w, raftpb.Snapshot{}, raftpb.HardState{}, nil, nil
	}

	snapData, err := loadRaftSnapshot(lg, walDir, ss)
	if err != nil {
		return nil, raftpb.Snapshot{}, raftpb.HardState{}, nil, err
	}

	w, err := wal.Open(lg, walDir, walSnapshot(snapData))
	if err != nil {
		return nil, raftpb.Snapshot{}, raftpb.HardState{}, nil, err
	}
	_, hs, ents, err := w.ReadAll()
	if err != nil {
		_ = w.Close()
		return nil, raftpb.Snapshot{}, raftpb.HardState{}, nil, err
	}
	return w, snapData, hs, ents, nil
}

func (r *raftLog) pruneSnapshots() error {
	if r.snapshotDir == "" || r.snapshotRetain <= 0 {
		return nil
	}

	ents, err := os.ReadDir(r.snapshotDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var names []string
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if strings.HasSuffix(name, raftSnapSuffix) {
			names = append(names, name)
		}
	}

	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	if len(names) <= r.snapshotRetain {
		return nil
	}

	for _, name := range names[r.snapshotRetain:] {
		path := filepath.Join(r.snapshotDir, name)
		if err := os.Remove(path); err != nil &&
			!os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func openSnapshotter(
	lg *zap.Logger, snapDir string,
) (*snap.Snapshotter, error) {
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, err
	}
	return snap.New(lg, snapDir), nil
}

func loadRaftSnapshot(
	lg *zap.Logger, walDir string, ss *snap.Snapshotter,
) (raftpb.Snapshot, error) {
	walSnaps, err := wal.ValidSnapshotEntries(lg, walDir)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	snapData, err := ss.LoadNewestAvailable(walSnaps)
	switch {
	case err == nil:
		return *snapData, nil
	case errors.Is(err, snap.ErrNoSnapshot):
		return raftpb.Snapshot{}, nil
	default:
		return raftpb.Snapshot{}, err
	}
}

func walSnapshot(snapData raftpb.Snapshot) walpb.Snapshot {
	if raft.IsEmptySnap(snapData) {
		return walpb.Snapshot{}
	}
	return walpb.Snapshot{
		Index:     snapData.Metadata.Index,
		Term:      snapData.Metadata.Term,
		ConfState: &snapData.Metadata.ConfState,
	}
}

func newEtcdLogger() *zap.Logger {
	lg := slog.Default()
	sink := slog.NewLogLogger(
		lg.With(slog.String("component", "timebox-raft-wal")).Handler(),
		slog.LevelWarn,
	).Writer()

	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = ""
	cfg.CallerKey = ""
	cfg.NameKey = ""
	cfg.StacktraceKey = ""

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(sink),
		zap.WarnLevel,
	)
	return zap.New(core)
}

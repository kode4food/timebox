package raft

import (
	"errors"
	"os"
	"sync"

	bin "github.com/kode4food/timebox/internal/binary"
	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	raftLog struct {
		mu sync.RWMutex

		logDir   string
		db       *bbolt.DB
		hs       raftpb.HardState
		cs       raftpb.ConfState
		segs     []logSeg
		prevSegs []uint64
		tailID   uint64
		last     uint64
		hot      tailCache

		compacted     uint64
		compactedTerm uint64
		snapshotFn    func() ([]byte, uint64, error)
		pendingSnap   chan snapResult

		nextID uint64
		logf   *os.File
		idxf   *os.File
	}

	snapResult struct {
		data    []byte
		applied uint64
		err     error
	}
)

const restorePageSize = 4 * 1024 * 1024

var _ raft.Storage = (*raftLog)(nil)

func (r *raftLog) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	errs = append(errs, r.closeTailLocked())
	errs = append(errs, r.db.Close())
	return errors.Join(errs...)
}

func (r *raftLog) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hs, r.cs, nil
}

func (r *raftLog) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	r.mu.RLock()
	last := r.last
	compacted := r.compacted
	if lo >= hi {
		r.mu.RUnlock()
		return nil, nil
	}
	if lo <= compacted {
		r.mu.RUnlock()
		return nil, raft.ErrCompacted
	}
	if hi > last+1 || lo > last {
		r.mu.RUnlock()
		return nil, raft.ErrUnavailable
	}
	if ents, ok := r.hot.entries(lo, hi, maxSize); ok {
		r.mu.RUnlock()
		return ents, nil
	}
	segs := cloneSegs(r.segs)
	r.mu.RUnlock()

	return loadEntries(r.logDir, segs, lo, hi, maxSize)
}

func (r *raftLog) Term(idx uint64) (uint64, error) {
	if idx == 0 {
		return 0, nil
	}

	r.mu.RLock()
	compacted := r.compacted
	compactedTerm := r.compactedTerm
	if idx < compacted {
		r.mu.RUnlock()
		return 0, raft.ErrCompacted
	}
	if idx == compacted {
		r.mu.RUnlock()
		return compactedTerm, nil
	}
	if term, ok := r.hot.term(idx); ok {
		r.mu.RUnlock()
		return term, nil
	}
	last := r.last
	segs := cloneSegs(r.segs)
	r.mu.RUnlock()

	if idx > last {
		return 0, raft.ErrUnavailable
	}
	i := findSeg(segs, idx)
	if i < 0 {
		return 0, raft.ErrCompacted
	}
	return readSegTerm(r.logDir, segs[i], idx)
}

func (r *raftLog) LastIndex() (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.last, nil
}

func (r *raftLog) FirstIndex() (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.compacted + 1, nil
}

func (r *raftLog) Snapshot() (raftpb.Snapshot, error) {
	if r.snapshotFn == nil {
		return raftpb.Snapshot{},
			raft.ErrSnapshotTemporarilyUnavailable
	}

	r.mu.Lock()
	ch := r.pendingSnap
	r.mu.Unlock()

	if ch != nil {
		select {
		case res := <-ch:
			r.mu.Lock()
			r.pendingSnap = nil
			r.mu.Unlock()
			if res.err != nil {
				return raftpb.Snapshot{}, res.err
			}
			return r.buildSnapshot(res.data, res.applied), nil
		default:
			return raftpb.Snapshot{},
				raft.ErrSnapshotTemporarilyUnavailable
		}
	}

	ch = make(chan snapResult, 1)
	r.mu.Lock()
	r.pendingSnap = ch
	r.mu.Unlock()

	fn := r.snapshotFn
	go func() {
		data, applied, err := fn()
		ch <- snapResult{data, applied, err}
	}()
	return raftpb.Snapshot{},
		raft.ErrSnapshotTemporarilyUnavailable
}

func (r *raftLog) buildSnapshot(data []byte, applied uint64) raftpb.Snapshot {
	term, _ := r.Term(applied)
	r.mu.RLock()
	cs := r.cs
	r.mu.RUnlock()
	return raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index:     applied,
			Term:      term,
			ConfState: cs,
		},
	}
}

func (r *raftLog) Save(rd raft.Ready, compactBound uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	hs := r.hs
	if !raft.IsEmptyHardState(rd.HardState) {
		hs = rd.HardState
	}

	var manifestDirty bool
	if len(rd.Entries) != 0 {
		var err error
		manifestDirty, err = r.appendLocked(rd.Entries)
		if err != nil {
			return err
		}
	}

	if hs != r.hs {
		if err := r.appendHardStateLocked(hs); err != nil {
			return err
		}
	}

	needSync := len(rd.Entries) != 0 || hs != r.hs
	if needSync {
		if err := r.syncLogLocked(); err != nil {
			return err
		}
	}

	rotated, err := r.rotateLocked(hs.Commit)
	if err != nil {
		return err
	}
	removed := r.compactLocked(compactBound)

	manifestDirty = manifestDirty || rotated || len(removed) > 0
	if err := r.storeMetaLocked(
		hs, r.cs, manifestDirty,
	); err != nil {
		return err
	}
	r.hs = hs

	for _, seg := range removed {
		_ = os.Remove(logFilePath(r.logDir, seg.id))
		_ = os.Remove(logIdxPath(r.logDir, seg.id))
	}
	return nil
}

func (r *raftLog) ApplySnapshot(meta raftpb.SnapshotMetadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if meta.Index <= r.compacted {
		return nil
	}

	r.compacted = meta.Index
	r.compactedTerm = meta.Term
	r.cs = meta.ConfState

	var removed []logSeg
	for len(r.segs) > 0 && r.segs[0].last <= meta.Index {
		removed = append(removed, r.segs[0])
		r.segs = r.segs[1:]
	}

	if len(r.segs) == 0 {
		r.last = meta.Index
		_ = r.closeTailLocked()
	}
	r.hot.reset()

	dirty := len(removed) > 0
	if err := r.storeMetaLocked(r.hs, r.cs, dirty); err != nil {
		return err
	}

	for _, seg := range removed {
		_ = os.Remove(logFilePath(r.logDir, seg.id))
		_ = os.Remove(logIdxPath(r.logDir, seg.id))
	}
	return nil
}

func (r *raftLog) SetConfState(cs raftpb.ConfState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.storeMetaLocked(r.hs, cs, false); err != nil {
		return err
	}
	r.cs = cs
	return nil
}

func (r *raftLog) CommitIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hs.Commit
}

func (r *raftLog) ReplayCommitted(
	after uint64, fn func([]raftpb.Entry) error,
) error {
	commit := r.CommitIndex()
	if after >= commit {
		return nil
	}
	for lo := after + 1; lo <= commit; {
		ents, err := r.Entries(lo, commit+1, restorePageSize)
		if err != nil {
			return err
		}
		if len(ents) == 0 {
			return bin.ErrCorruptState
		}
		if err := fn(ents); err != nil {
			return err
		}
		lo = ents[len(ents)-1].Index + 1
	}
	return nil
}

func (r *raftLog) appendLocked(ents []raftpb.Entry) (bool, error) {
	if len(ents) == 0 {
		return false, nil
	}

	var manifest bool
	first := ents[0].Index
	switch {
	case len(r.segs) == 0:
		if first != r.compacted+1 {
			return false, bin.ErrCorruptState
		}
		if err := r.startTailLocked(first); err != nil {
			return false, err
		}
		manifest = true
	case first > r.last+1:
		return false, bin.ErrCorruptState
	case first <= r.last:
		if first <= r.hs.Commit {
			return false, bin.ErrCorruptState
		}
		if err := r.trimTailLocked(first); err != nil {
			return false, err
		}
	}

	if r.logf == nil || r.idxf == nil {
		return false, bin.ErrCorruptState
	}

	seg := &r.segs[len(r.segs)-1]
	var logBuf []byte
	var idxBuf []byte

	for _, ent := range ents {
		if ent.Index != r.last+1 {
			return false, bin.ErrCorruptState
		}
		off := seg.bytes + int64(len(logBuf))
		var err error
		logBuf, err = appendFrame(logBuf, ent)
		if err != nil {
			return false, err
		}
		if len(seg.pts) == 0 ||
			(ent.Index-seg.first)%logPointSpan == 0 {
			pt := logPoint{idx: ent.Index, off: off}
			idxBuf = appendIdxRecord(idxBuf, pt)
			seg.pts = append(seg.pts, pt)
		}
		seg.last = ent.Index
		seg.lastTerm = ent.Term
		r.last = ent.Index
		r.hot.put(ent)
	}

	if _, err := r.logf.Write(logBuf); err != nil {
		return false, err
	}
	seg.bytes += int64(len(logBuf))
	if len(idxBuf) != 0 {
		if _, err := r.idxf.Write(idxBuf); err != nil {
			return false, err
		}
	}
	return manifest, nil
}

func (r *raftLog) appendHardStateLocked(hs raftpb.HardState) error {
	if r.logf == nil {
		return nil
	}
	frame := appendHardStateFrame(nil, hs)
	_, err := r.logf.Write(frame)
	if err != nil {
		return err
	}
	if len(r.segs) != 0 {
		r.segs[len(r.segs)-1].bytes += int64(len(frame))
	}
	return nil
}

func (r *raftLog) syncLogLocked() error {
	if r.logf == nil {
		return nil
	}
	return r.logf.Sync()
}

func (r *raftLog) warmHotTail() error {
	if r.last == 0 {
		return nil
	}

	lo := r.compacted + 1
	if r.last > uint64(len(r.hot.buf)) {
		lo = max(lo, r.last-uint64(len(r.hot.buf))+1)
	}
	ents, err := loadEntries(
		r.logDir, cloneSegs(r.segs), lo, r.last+1,
		^uint64(0),
	)
	if err != nil {
		return err
	}
	for _, ent := range ents {
		r.hot.put(ent)
	}
	return nil
}

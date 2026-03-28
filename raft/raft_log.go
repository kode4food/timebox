package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type (
	raftLog struct {
		mu sync.RWMutex

		logDir    string
		statePath string
		db        *bbolt.DB
		hs        raftpb.HardState
		cs        raftpb.ConfState
		segs      []logSeg
		tailID    uint64
		last      uint64
		hot       tailCache

		nextID uint64
		logf   *os.File
		idxf   *os.File
	}

	logSeg struct {
		id    uint64
		first uint64
		last  uint64
		bytes int64
		pts   []logPoint
	}

	logPoint struct {
		idx uint64
		off int64
	}

	protoMarshaler interface {
		Marshal() ([]byte, error)
	}

	protoUnmarshaler interface {
		Unmarshal([]byte) error
	}

	tailCache struct {
		buf   []raftpb.Entry
		head  int
		n     int
		first uint64
	}
)

const (
	walMetaDirName  = "meta"
	walMetaFileName = "wal-meta.db"
	walStateFile    = "wal-state.bin"

	walLogDirName = "log"
	walLogExt     = ".log"
	walIdxExt     = ".idx"

	logRotateBytes = 128 * 1024 * 1024
	logPointSpan   = 256

	frameMaxBody = 256 * 1024 * 1024

	idxRecordSize   = 16
	logReadBufSize  = 64 * 1024
	restorePageSize = 4 * 1024 * 1024
)

var (
	logMetaBucket = []byte("wal-meta")
	logSegBucket  = []byte("wal-segments")

	currentTermKey = []byte("current-term")
	votedForKey    = []byte("voted-for")
	confStateKey   = []byte("conf-state")
	tailSegKey     = []byte("tail-segment")
)

var _ raft.Storage = (*raftLog)(nil)

func openRaftLog(cfg Config) (*raftLog, bool, error) {
	dataDir := cfg.DataDir
	metaDir := filepath.Join(dataDir, walMetaDirName)
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return nil, false, err
	}

	logDir := filepath.Join(dataDir, walLogDirName)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, false, err
	}
	statePath := filepath.Join(metaDir, walStateFile)

	db, err := bbolt.Open(filepath.Join(metaDir, walMetaFileName), 0o600,
		&bbolt.Options{
			Timeout:        time.Second,
			FreelistType:   bbolt.FreelistMapType,
			NoFreelistSync: true,
		},
	)
	if err != nil {
		return nil, false, err
	}
	if err := initRaftMetaDB(db); err != nil {
		_ = db.Close()
		return nil, false, err
	}

	hs, cs, tailID, segs, err := loadRaftMeta(db, statePath)
	if err != nil {
		_ = db.Close()
		return nil, false, err
	}

	opened, last, tailID, err := openLogSegs(logDir, segs, tailID)
	if err != nil {
		_ = db.Close()
		return nil, false, err
	}
	if hs.Commit > last {
		_ = db.Close()
		return nil, false, ErrCorruptState
	}

	lg := &raftLog{
		logDir:    logDir,
		statePath: statePath,
		db:        db,
		hs:        hs,
		cs:        cs,
		segs:      opened,
		tailID:    tailID,
		last:      last,
		hot:       newTailCache(cfg.RecentEntriesSize),
		nextID:    nextLogID(logDir, opened),
	}
	if err := lg.openTail(); err != nil {
		_ = db.Close()
		return nil, false, err
	}
	if err := lg.warmHotTail(); err != nil {
		_ = lg.Close()
		return nil, false, err
	}

	stateExists := last != 0 ||
		!raft.IsEmptyHardState(hs) ||
		!emptyConfState(cs)
	return lg, stateExists, nil
}

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
	if lo >= hi {
		r.mu.RUnlock()
		return nil, nil
	}
	if lo < 1 {
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
		return 0, ErrCorruptState
	}
	return readSegTerm(r.logDir, segs[i], idx)
}

func (r *raftLog) LastIndex() (uint64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.last, nil
}

func (r *raftLog) FirstIndex() (uint64, error) {
	return 1, nil
}

func (r *raftLog) Snapshot() (raftpb.Snapshot, error) {
	return raftpb.Snapshot{}, ErrSnapshotUnsupported
}

func (r *raftLog) Save(rd raft.Ready) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var manifestDirty bool
	if len(rd.Entries) != 0 {
		var err error
		manifestDirty, err = r.appendLocked(rd.Entries)
		if err != nil {
			return err
		}
		if err := r.syncLogLocked(); err != nil {
			return err
		}
	}

	hs := r.hs
	if !raft.IsEmptyHardState(rd.HardState) {
		hs = rd.HardState
	}
	if hs != r.hs {
		if err := storeHardState(r.statePath, hs); err != nil {
			return err
		}
	}
	if err := r.storeMetaLocked(hs, r.cs, manifestDirty); err != nil {
		return err
	}
	r.hs = hs

	rotated, err := r.rotateLocked(hs.Commit)
	if err != nil {
		return err
	}
	if rotated {
		return r.storeMetaLocked(r.hs, r.cs, true)
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
			return ErrCorruptState
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
		if first != 1 {
			return false, ErrCorruptState
		}
		if err := r.startTailLocked(first); err != nil {
			return false, err
		}
		manifest = true
	case first > r.last+1:
		return false, ErrCorruptState
	case first <= r.last:
		if first <= r.hs.Commit {
			return false, ErrCorruptState
		}
		if err := r.trimTailLocked(first); err != nil {
			return false, err
		}
	}

	for _, ent := range ents {
		if ent.Index != r.last+1 {
			return false, ErrCorruptState
		}
		if err := r.appendEntryLocked(ent); err != nil {
			return false, err
		}
		r.last = ent.Index
	}
	return manifest, nil
}

func (r *raftLog) appendEntryLocked(ent raftpb.Entry) error {
	if r.logf == nil || r.idxf == nil {
		return ErrCorruptState
	}

	seg := &r.segs[len(r.segs)-1]
	off := seg.bytes
	frame, err := marshalFrame(ent)
	if err != nil {
		return err
	}
	if _, err := r.logf.Write(frame); err != nil {
		return err
	}
	if len(seg.pts) == 0 || (ent.Index-seg.first)%logPointSpan == 0 {
		pt := logPoint{idx: ent.Index, off: off}
		if err := writeIdxPoint(r.idxf, pt); err != nil {
			return err
		}
		seg.pts = append(seg.pts, pt)
	}

	seg.last = ent.Index
	seg.bytes += int64(len(frame))
	r.hot.put(ent)
	return nil
}

func (r *raftLog) trimTailLocked(first uint64) error {
	if len(r.segs) == 0 {
		return ErrCorruptState
	}

	seg := &r.segs[len(r.segs)-1]
	if first < seg.first || first > seg.last+1 {
		return ErrCorruptState
	}
	if first == seg.last+1 {
		return nil
	}

	var off int64
	var err error
	if first > seg.first {
		off, err = segOffset(r.logDir, *seg, first)
		if err != nil {
			return err
		}
	}
	if err := r.logf.Truncate(off); err != nil {
		return err
	}
	if _, err := r.logf.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	seg.bytes = off
	seg.last = first - 1
	seg.pts = trimPts(seg.pts, first)
	if err := rewriteIdx(logIdxPath(r.logDir, seg.id), seg.pts); err != nil {
		return err
	}
	if err := r.reopenTailIdxLocked(seg.id); err != nil {
		return err
	}

	r.last = lastIndex(r.segs)
	r.hot.truncate(first)
	return nil
}

func (r *raftLog) rotateLocked(commit uint64) (bool, error) {
	if len(r.segs) == 0 {
		return false, nil
	}

	seg := r.segs[len(r.segs)-1]
	if seg.bytes < logRotateBytes || seg.last < seg.first ||
		commit < seg.last {
		return false, nil
	}
	if err := r.closeTailLocked(); err != nil {
		return false, err
	}
	if err := r.startTailLocked(seg.last + 1); err != nil {
		return false, err
	}
	return true, nil
}

func (r *raftLog) startTailLocked(first uint64) error {
	id := r.nextID
	if id == 0 {
		id = 1
	}

	logPath := logFilePath(r.logDir, id)
	logf, err := os.OpenFile(logPath,
		os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600,
	)
	if err != nil {
		return err
	}
	idxPath := logIdxPath(r.logDir, id)
	idxf, err := os.OpenFile(idxPath,
		os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600,
	)
	if err != nil {
		_ = logf.Close()
		return err
	}

	r.logf = logf
	r.idxf = idxf
	r.tailID = id
	r.nextID = id + 1
	r.segs = append(r.segs, logSeg{
		id:    id,
		first: first,
		last:  first - 1,
	})
	return nil
}

func (r *raftLog) openTail() error {
	if len(r.segs) == 0 || r.tailID == 0 {
		return nil
	}
	id := r.tailID
	logf, err := os.OpenFile(logFilePath(r.logDir, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600,
	)
	if err != nil {
		return err
	}
	idxf, err := os.OpenFile(logIdxPath(r.logDir, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600,
	)
	if err != nil {
		_ = logf.Close()
		return err
	}
	r.logf = logf
	r.idxf = idxf
	return nil
}

func (r *raftLog) reopenTailIdxLocked(id uint64) error {
	if r.idxf != nil {
		_ = r.idxf.Close()
		r.idxf = nil
	}
	idxf, err := os.OpenFile(logIdxPath(r.logDir, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600,
	)
	if err != nil {
		return err
	}
	r.idxf = idxf
	return nil
}

func (r *raftLog) syncLogLocked() error {
	if r.logf == nil {
		return nil
	}
	return r.logf.Sync()
}

func (r *raftLog) closeTailLocked() error {
	var errs []error
	if r.idxf != nil {
		errs = append(errs, r.idxf.Sync(), r.idxf.Close())
		r.idxf = nil
	}
	if r.logf != nil {
		errs = append(errs, r.logf.Sync(), r.logf.Close())
		r.logf = nil
	}
	return errors.Join(errs...)
}

func (r *raftLog) storeMetaLocked(
	hs raftpb.HardState, cs raftpb.ConfState, manifest bool,
) error {
	if !manifest &&
		termVoteEqual(r.hs, hs) &&
		confStateEqual(r.cs, cs) {
		return nil
	}

	return r.db.Update(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(logMetaBucket)
		if !termVoteEqual(r.hs, hs) {
			if err := mb.Put(
				currentTermKey, putU64(nil, hs.Term),
			); err != nil {
				return err
			}
			if err := mb.Put(
				votedForKey, putU64(nil, hs.Vote),
			); err != nil {
				return err
			}
		}
		if !confStateEqual(r.cs, cs) {
			if err := putProto(mb, confStateKey, &cs); err != nil {
				return err
			}
		}
		if !manifest {
			return nil
		}

		sb := tx.Bucket(logSegBucket)
		if err := clearBucket(sb); err != nil {
			return err
		}
		for _, seg := range r.segs {
			if err := sb.Put(
				putU64(nil, seg.first), putU64(nil, seg.id),
			); err != nil {
				return err
			}
		}
		return mb.Put(tailSegKey, putU64(nil, r.tailID))
	})
}

func (r *raftLog) warmHotTail() error {
	if r.last == 0 {
		return nil
	}

	lo := uint64(1)
	if r.last > uint64(len(r.hot.buf)) {
		lo = r.last - uint64(len(r.hot.buf)) + 1
	}
	ents, err := loadEntries(r.logDir, cloneSegs(r.segs), lo, r.last+1,
		^uint64(0))
	if err != nil {
		return err
	}
	for _, ent := range ents {
		r.hot.put(ent)
	}
	return nil
}

func initRaftMetaDB(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(logMetaBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(logSegBucket)
		return err
	})
}

func loadRaftMeta(
	db *bbolt.DB, statePath string,
) (raftpb.HardState, raftpb.ConfState, uint64, []logSeg, error) {
	var hs raftpb.HardState
	var cs raftpb.ConfState
	var tailID uint64
	var segs []logSeg

	err := db.View(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(logMetaBucket)
		sb := tx.Bucket(logSegBucket)

		if err := loadProto(mb, confStateKey, &cs); err != nil {
			return err
		}
		if hs.Term == 0 {
			if v := mb.Get(currentTermKey); len(v) != 0 {
				term, err := getU64(v)
				if err != nil {
					return err
				}
				hs.Term = term
			}
		}
		if hs.Vote == 0 {
			if v := mb.Get(votedForKey); len(v) != 0 {
				vote, err := getU64(v)
				if err != nil {
					return err
				}
				hs.Vote = vote
			}
		}
		if v := mb.Get(tailSegKey); len(v) != 0 {
			id, err := getU64(v)
			if err != nil {
				return err
			}
			tailID = id
		}

		c := sb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			first, err := getU64(k)
			if err != nil {
				return err
			}
			id, err := getU64(v)
			if err != nil {
				return err
			}
			segs = append(segs, logSeg{
				id:    id,
				first: first,
				last:  first - 1,
			})
		}
		return nil
	})
	if err != nil {
		return hs, cs, tailID, segs, err
	}

	st, err := loadHardState(statePath)
	if err != nil {
		return hs, cs, tailID, segs, err
	}
	if st.Term != 0 {
		hs.Term = st.Term
	}
	if st.Vote != 0 {
		hs.Vote = st.Vote
	}
	if st.Commit != 0 {
		hs.Commit = st.Commit
	}
	return hs, cs, tailID, segs, err
}

func openLogSegs(
	dir string, segs []logSeg, tailID uint64,
) ([]logSeg, uint64, uint64, error) {
	if len(segs) == 0 {
		return nil, 0, 0, nil
	}
	if tailID == 0 {
		tailID = segs[len(segs)-1].id
	}
	if segs[len(segs)-1].id != tailID {
		return nil, 0, 0, ErrCorruptState
	}

	var res = make([]logSeg, 0, len(segs))
	var last uint64
	for i, seg := range segs {
		allowTrunc := seg.id == tailID
		opened, err := scanSeg(dir, seg.id, seg.first, allowTrunc)
		if err != nil {
			return nil, 0, 0, err
		}
		switch {
		case i == 0 && opened.first != 1:
			return nil, 0, 0, ErrCorruptState
		case i != 0 && opened.first != last+1:
			return nil, 0, 0, ErrCorruptState
		case !allowTrunc && opened.last < opened.first:
			return nil, 0, 0, ErrCorruptState
		}
		last = max(last, opened.last)
		res = append(res, opened)
	}
	return res, last, tailID, nil
}

func scanSeg(dir string, id, first uint64, allowTrunc bool) (logSeg, error) {
	path := logFilePath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return logSeg{}, err
	}
	defer func() { _ = f.Close() }()

	rd := bufio.NewReaderSize(f, logReadBufSize)
	var pts []logPoint
	var off int64
	var last = first - 1
	var want = first
	for {
		ent, n, err := readLogFrame(rd)
		switch {
		case err == nil:
			if ent.Index != want {
				if allowTrunc {
					goto repair
				}
				return logSeg{}, ErrCorruptState
			}
			if len(pts) == 0 || (ent.Index-first)%logPointSpan == 0 {
				pts = append(pts, logPoint{
					idx: ent.Index,
					off: off,
				})
			}
			last = ent.Index
			want++
			off += n
		case errors.Is(err, io.EOF):
			goto done
		default:
			if allowTrunc {
				goto repair
			}
			return logSeg{}, err
		}
	}

repair:
	if err := f.Truncate(off); err != nil {
		return logSeg{}, err
	}

done:
	if err := rewriteIdx(logIdxPath(dir, id), pts); err != nil {
		return logSeg{}, err
	}
	return logSeg{
		id:    id,
		first: first,
		last:  last,
		bytes: off,
		pts:   pts,
	}, nil
}

func loadEntries(
	dir string, segs []logSeg, lo, hi, maxSize uint64,
) ([]raftpb.Entry, error) {
	var ents []raftpb.Entry
	var total uint64
	var limited bool
	for idx := lo; idx < hi && !limited; {
		i := findSeg(segs, idx)
		if i < 0 {
			return nil, ErrCorruptState
		}
		next, size, stop, err := readSegEntries(
			dir, segs[i], idx, hi, maxSize, total, &ents,
		)
		if err != nil {
			return nil, err
		}
		idx = next
		total = size
		limited = stop
	}
	if limited || len(ents) != 0 {
		return ents[:len(ents):len(ents)], nil
	}
	return nil, ErrCorruptState
}

func readSegEntries(
	dir string, seg logSeg, lo, hi, maxSize, total uint64, ents *[]raftpb.Entry,
) (uint64, uint64, bool, error) {
	if seg.last < seg.first {
		return lo, total, false, nil
	}

	path := logFilePath(dir, seg.id)
	pt := findPt(seg.pts, lo)

	f, err := os.Open(path)
	if err != nil {
		return lo, total, false, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(pt.off, io.SeekStart); err != nil {
		return lo, total, false, err
	}
	rd := bufio.NewReaderSize(f, logReadBufSize)

	want := lo
	off := pt.off
	for want < hi && off < seg.bytes {
		ent, n, err := readLogFrame(rd)
		if err != nil {
			return want, total, false, err
		}
		off += n
		switch {
		case ent.Index < want:
			continue
		case ent.Index != want:
			return want, total, false, ErrCorruptState
		case len(*ents) != 0 && total+uint64(ent.Size()) > maxSize:
			return want, total, true, nil
		}
		*ents = append(*ents, cloneEntry(ent))
		total += uint64(ent.Size())
		want++
	}
	if want < min(hi, seg.last+1) {
		return want, total, false, ErrCorruptState
	}
	return want, total, false, nil
}

func readSegTerm(dir string, seg logSeg, idx uint64) (uint64, error) {
	if seg.last < seg.first {
		return 0, ErrCorruptState
	}
	pt := findPt(seg.pts, idx)

	f, err := os.Open(logFilePath(dir, seg.id))
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(pt.off, io.SeekStart); err != nil {
		return 0, err
	}
	rd := bufio.NewReaderSize(f, logReadBufSize)

	for off := pt.off; off < seg.bytes; {
		ent, n, err := readLogFrame(rd)
		if err != nil {
			return 0, err
		}
		off += n
		switch {
		case ent.Index < idx:
			continue
		case ent.Index == idx:
			return ent.Term, nil
		default:
			return 0, ErrCorruptState
		}
	}
	return 0, ErrCorruptState
}

func segOffset(dir string, seg logSeg, idx uint64) (int64, error) {
	if idx == seg.first {
		return 0, nil
	}

	pt := findPt(seg.pts, idx)
	f, err := os.Open(logFilePath(dir, seg.id))
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(pt.off, io.SeekStart); err != nil {
		return 0, err
	}
	rd := bufio.NewReaderSize(f, logReadBufSize)

	off := pt.off
	for off < seg.bytes {
		ent, n, err := readLogFrame(rd)
		if err != nil {
			return 0, err
		}
		if ent.Index == idx {
			return off, nil
		}
		if ent.Index > idx {
			return 0, ErrCorruptState
		}
		off += n
	}
	return 0, ErrCorruptState
}

func marshalFrame(ent raftpb.Entry) ([]byte, error) {
	if len(ent.Data) > frameMaxBody {
		return nil, ErrCorruptState
	}

	bodyLen := 1 + 8 + 8 + len(ent.Data)
	buf := make([]byte, 4+bodyLen+4)
	binary.BigEndian.PutUint32(buf[:4], uint32(bodyLen))
	buf[4] = byte(ent.Type)
	binary.BigEndian.PutUint64(buf[5:13], ent.Index)
	binary.BigEndian.PutUint64(buf[13:21], ent.Term)
	copy(buf[21:21+len(ent.Data)], ent.Data)
	sum := crc32.ChecksumIEEE(buf[4 : 4+bodyLen])
	binary.BigEndian.PutUint32(buf[4+bodyLen:], sum)
	return buf, nil
}

func readLogFrame(r *bufio.Reader) (raftpb.Entry, int64, error) {
	var hdr [4]byte
	n, err := io.ReadFull(r, hdr[:])
	switch {
	case err == nil:
	case errors.Is(err, io.EOF) && n == 0:
		return raftpb.Entry{}, 0, io.EOF
	default:
		return raftpb.Entry{}, 0, ErrCorruptState
	}

	bodyLen := binary.BigEndian.Uint32(hdr[:])
	switch {
	case bodyLen < 17:
		return raftpb.Entry{}, 0, ErrCorruptState
	case bodyLen > frameMaxBody:
		return raftpb.Entry{}, 0, ErrCorruptState
	}

	body := make([]byte, int(bodyLen)+4)
	if _, err := io.ReadFull(r, body); err != nil {
		return raftpb.Entry{}, 0, ErrCorruptState
	}
	want := binary.BigEndian.Uint32(body[len(body)-4:])
	if crc32.ChecksumIEEE(body[:len(body)-4]) != want {
		return raftpb.Entry{}, 0, ErrCorruptState
	}
	ent := raftpb.Entry{
		Type:  raftpb.EntryType(body[0]),
		Index: binary.BigEndian.Uint64(body[1:9]),
		Term:  binary.BigEndian.Uint64(body[9:17]),
		Data:  append([]byte(nil), body[17:len(body)-4]...),
	}
	return ent, int64(4 + len(body)), nil
}

func writeIdxPoint(f *os.File, pt logPoint) error {
	var buf [idxRecordSize]byte
	binary.BigEndian.PutUint64(buf[:8], pt.idx)
	binary.BigEndian.PutUint64(buf[8:16], uint64(pt.off))
	_, err := f.Write(buf[:])
	return err
}

func rewriteIdx(path string, pts []logPoint) error {
	f, err := os.OpenFile(path,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600,
	)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	for _, pt := range pts {
		if err := writeIdxPoint(f, pt); err != nil {
			return err
		}
	}
	return f.Sync()
}

func findSeg(segs []logSeg, idx uint64) int {
	i := sort.Search(len(segs), func(i int) bool {
		return segs[i].last >= idx
	})
	if i == len(segs) || idx < segs[i].first {
		return -1
	}
	return i
}

func findPt(pts []logPoint, idx uint64) logPoint {
	i := sort.Search(len(pts), func(i int) bool {
		return pts[i].idx > idx
	})
	if i == 0 {
		return pts[0]
	}
	return pts[i-1]
}

func trimPts(pts []logPoint, first uint64) []logPoint {
	i := sort.Search(len(pts), func(i int) bool {
		return pts[i].idx >= first
	})
	return append([]logPoint(nil), pts[:i]...)
}

func cloneSegs(segs []logSeg) []logSeg {
	if len(segs) == 0 {
		return nil
	}
	res := make([]logSeg, len(segs))
	for i, seg := range segs {
		res[i] = seg
		res[i].pts = append([]logPoint(nil), seg.pts...)
	}
	return res
}

func lastIndex(segs []logSeg) uint64 {
	var last uint64
	for _, seg := range segs {
		last = max(last, seg.last)
	}
	return last
}

func nextLogID(dir string, segs []logSeg) uint64 {
	var next uint64 = 1
	for _, seg := range segs {
		next = max(next, seg.id+1)
	}

	ents, err := os.ReadDir(dir)
	if err != nil {
		return next
	}
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.HasSuffix(name, walLogExt) {
			continue
		}
		id, err := strconv.ParseUint(
			strings.TrimSuffix(name, walLogExt), 10, 64,
		)
		if err != nil {
			continue
		}
		next = max(next, id+1)
	}
	return next
}

func logFilePath(dir string, id uint64) string {
	return filepath.Join(dir, strconv.FormatUint(id, 10)+walLogExt)
}

func logIdxPath(dir string, id uint64) string {
	return filepath.Join(dir, strconv.FormatUint(id, 10)+walIdxExt)
}

func clearBucket(b *bbolt.Bucket) error {
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if err := c.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func putProto(b *bbolt.Bucket, key []byte, m protoMarshaler) error {
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return b.Put(key, data)
}

func loadProto(b *bbolt.Bucket, key []byte, m protoUnmarshaler) error {
	data := b.Get(key)
	if len(data) == 0 {
		return nil
	}
	return m.Unmarshal(data)
}

func putU64(dst []byte, v uint64) []byte {
	if len(dst) < 8 {
		dst = make([]byte, 8)
	}
	binary.BigEndian.PutUint64(dst[:8], v)
	return dst[:8]
}

func getU64(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, ErrCorruptState
	}
	return binary.BigEndian.Uint64(data), nil
}

func loadHardState(path string) (raftpb.HardState, error) {
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
	case os.IsNotExist(err):
		return raftpb.HardState{}, nil
	default:
		return raftpb.HardState{}, err
	}
	if len(data) == 0 {
		return raftpb.HardState{}, nil
	}

	var hs raftpb.HardState
	if err := hs.Unmarshal(data); err != nil {
		return raftpb.HardState{}, ErrCorruptState
	}
	return hs, nil
}

func storeHardState(path string, hs raftpb.HardState) error {
	data, err := hs.Marshal()
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func confStateEqual(a, b raftpb.ConfState) bool {
	return a.Equivalent(b) == nil
}

func termVoteEqual(a, b raftpb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote
}

func newTailCache(n int) tailCache {
	return tailCache{
		buf: make([]raftpb.Entry, n),
	}
}

func (c *tailCache) entries(
	lo, hi, maxSize uint64,
) ([]raftpb.Entry, bool) {
	if !c.has(lo) {
		return nil, false
	}
	last := c.first + uint64(c.n) - 1
	if hi != 0 && hi-1 > last {
		return nil, false
	}

	ents := make([]raftpb.Entry, 0, int(hi-lo))
	var total uint64
	pos := (c.head + int(lo-c.first)) % len(c.buf)
	for idx := lo; idx < hi; idx++ {
		ent := c.buf[pos]
		if len(ents) != 0 && total+uint64(ent.Size()) > maxSize {
			break
		}
		ents = append(ents, cloneEntry(ent))
		total += uint64(ent.Size())
		pos++
		if pos == len(c.buf) {
			pos = 0
		}
	}
	return ents[:len(ents):len(ents)], true
}

func (c *tailCache) term(idx uint64) (uint64, bool) {
	if !c.has(idx) {
		return 0, false
	}
	return c.at(idx).Term, true
}

func (c *tailCache) put(ent raftpb.Entry) {
	if len(c.buf) == 0 {
		return
	}
	if c.n == 0 {
		c.first = ent.Index
		c.n = 1
		c.buf[0] = cloneEntry(ent)
		return
	}

	last := c.first + uint64(c.n) - 1
	if ent.Index != last+1 {
		c.reset()
		c.put(ent)
		return
	}
	if c.n == len(c.buf) {
		c.buf[c.head] = cloneEntry(ent)
		c.head = (c.head + 1) % len(c.buf)
		c.first++
		return
	}
	pos := (c.head + c.n) % len(c.buf)
	c.buf[pos] = cloneEntry(ent)
	c.n++
}

func (c *tailCache) truncate(first uint64) {
	if c.n == 0 {
		return
	}
	if first <= c.first {
		c.reset()
		return
	}

	last := c.first + uint64(c.n) - 1
	if first > last {
		return
	}
	c.n = int(first - c.first)
}

func (c *tailCache) reset() {
	c.head = 0
	c.n = 0
	c.first = 0
}

func (c *tailCache) has(idx uint64) bool {
	if c.n == 0 {
		return false
	}
	last := c.first + uint64(c.n) - 1
	return idx >= c.first && idx <= last
}

func (c *tailCache) at(idx uint64) raftpb.Entry {
	pos := (c.head + int(idx-c.first)) % len(c.buf)
	return c.buf[pos]
}

func cloneEntry(ent raftpb.Entry) raftpb.Entry {
	cp := ent
	cp.Data = append([]byte(nil), ent.Data...)
	return cp
}

func emptyConfState(cs raftpb.ConfState) bool {
	return len(cs.Voters) == 0 &&
		len(cs.VotersOutgoing) == 0 &&
		len(cs.Learners) == 0 &&
		len(cs.LearnersNext) == 0 &&
		!cs.AutoLeave
}

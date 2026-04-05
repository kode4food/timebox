package raft

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.etcd.io/raft/v3/raftpb"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	logSeg struct {
		id       uint64
		first    uint64
		last     uint64
		lastTerm uint64
		bytes    int64
		pts      []logPoint
	}

	logPoint struct {
		idx uint64
		off int64
	}
)

const (
	walLogDirName = "log"
	walLogExt     = ".log"
	walIdxExt     = ".idx"

	logPointSpan   = 256
	logReadBufSize = 64 * 1024
)

var logRotateBytes int64 = 128 * 1024 * 1024

func openLogSegs(
	dir string, segs []logSeg, tailID, compacted uint64,
) ([]logSeg, uint64, uint64, *raftpb.HardState, error) {
	if len(segs) == 0 {
		return nil, compacted, 0, nil, nil
	}
	if tailID == 0 {
		tailID = segs[len(segs)-1].id
	}
	if segs[len(segs)-1].id != tailID {
		return nil, 0, 0, nil, bin.ErrCorruptState
	}

	res := make([]logSeg, 0, len(segs))
	var last uint64
	var walHS *raftpb.HardState
	for i, seg := range segs {
		allowTrunc := seg.id == tailID
		opened, hs, err := scanSeg(dir, seg.id, seg.first, allowTrunc)
		if err != nil {
			return nil, 0, 0, nil, err
		}
		if hs != nil {
			walHS = hs
		}
		expect := compacted + 1
		if i != 0 {
			expect = last + 1
		}
		switch {
		case opened.first != expect:
			return nil, 0, 0, nil, bin.ErrCorruptState
		case !allowTrunc && opened.last < opened.first:
			return nil, 0, 0, nil, bin.ErrCorruptState
		}
		last = max(last, opened.last)
		res = append(res, opened)
	}
	return res, last, tailID, walHS, nil
}

func (r *raftLog) trimTailLocked(first uint64) error {
	if len(r.segs) == 0 {
		return bin.ErrCorruptState
	}

	seg := &r.segs[len(r.segs)-1]
	if first < seg.first || first > seg.last+1 {
		return bin.ErrCorruptState
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

func (r *raftLog) compactLocked(bound uint64) []logSeg {
	if bound == 0 || len(r.segs) < 3 {
		return nil
	}

	n := 0
	limit := len(r.segs) - 2
	for n < limit && r.segs[n].last < bound {
		n++
	}
	if n == 0 {
		return nil
	}

	removed := make([]logSeg, n)
	copy(removed, r.segs[:n])

	last := removed[n-1]
	r.compacted = last.last
	r.compactedTerm = last.lastTerm
	r.segs = r.segs[n:]
	return removed
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

func scanSeg(
	dir string, id, first uint64, allowTrunc bool,
) (logSeg, *raftpb.HardState, error) {
	path := logFilePath(dir, id)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return logSeg{}, nil, err
	}
	defer func() { _ = f.Close() }()

	rd := bufio.NewReaderSize(f, logReadBufSize)
	var pts []logPoint
	var off int64
	var last = first - 1
	var lastTerm uint64
	var want = first
	var walHS *raftpb.HardState
	for {
		ent, hs, n, err := readWALFrame(rd)
		switch {
		case err == nil:
			if hs != nil {
				walHS = hs
				off += n
				continue
			}
			if ent.Index != want {
				if allowTrunc {
					goto repair
				}
				return logSeg{}, nil, bin.ErrCorruptState
			}
			if len(pts) == 0 ||
				(ent.Index-first)%logPointSpan == 0 {
				pts = append(pts, logPoint{
					idx: ent.Index,
					off: off,
				})
			}
			last = ent.Index
			lastTerm = ent.Term
			want++
			off += n
		case errors.Is(err, io.EOF):
			goto done
		default:
			if allowTrunc {
				goto repair
			}
			return logSeg{}, nil, err
		}
	}

repair:
	if err := f.Truncate(off); err != nil {
		return logSeg{}, nil, err
	}

done:
	if err := rewriteIdx(logIdxPath(dir, id), pts); err != nil {
		return logSeg{}, nil, err
	}
	return logSeg{
		id:       id,
		first:    first,
		last:     last,
		lastTerm: lastTerm,
		bytes:    off,
		pts:      pts,
	}, walHS, nil
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
			return nil, bin.ErrCorruptState
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
	return nil, bin.ErrCorruptState
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
			return want, total, false, bin.ErrCorruptState
		case len(*ents) != 0 && total+uint64(ent.Size()) > maxSize:
			return want, total, true, nil
		}
		*ents = append(*ents, cloneEntry(ent))
		total += uint64(ent.Size())
		want++
	}
	if want < min(hi, seg.last+1) {
		return want, total, false, bin.ErrCorruptState
	}
	return want, total, false, nil
}

func readSegTerm(dir string, seg logSeg, idx uint64) (uint64, error) {
	if seg.last < seg.first {
		return 0, bin.ErrCorruptState
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
			return 0, bin.ErrCorruptState
		}
	}
	return 0, bin.ErrCorruptState
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
			return 0, bin.ErrCorruptState
		}
		off += n
	}
	return 0, bin.ErrCorruptState
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

func segFirsts(segs []logSeg) []uint64 {
	res := make([]uint64, len(segs))
	for i, s := range segs {
		res[i] = s.first
	}
	return res
}

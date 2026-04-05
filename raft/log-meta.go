package raft

import (
	"os"
	"path/filepath"
	"slices"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	protoMarshaler interface {
		Marshal() ([]byte, error)
	}

	protoUnmarshaler interface {
		Unmarshal([]byte) error
	}

	raftMeta struct {
		hs            raftpb.HardState
		cs            raftpb.ConfState
		tailID        uint64
		compacted     uint64
		compactedTerm uint64
		segs          []logSeg
	}
)

const (
	walMetaDirName  = "meta"
	walMetaFileName = "wal-meta"
)

var (
	logMetaBucket = []byte("wal-meta")
	logSegBucket  = []byte("wal-segments")

	currentTermKey = []byte("current-term")
	votedForKey    = []byte("voted-for")
	commitKey      = []byte("commit")
	confStateKey   = []byte("conf-state")
	tailSegKey     = []byte("tail-segment")
	compactedKey   = []byte("compacted")
)

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

	db, err := openKVDB(filepath.Join(metaDir, walMetaFileName))
	if err != nil {
		return nil, false, err
	}
	m, err := loadRaftMeta(db)
	if err != nil {
		_ = db.Close()
		return nil, false, err
	}

	opened, last, tailID, walHS, err := openLogSegs(
		logDir, m.segs, m.tailID, m.compacted,
	)
	if err != nil {
		_ = db.Close()
		return nil, false, err
	}

	hs := m.hs
	if walHS != nil {
		if walHS.Commit > hs.Commit {
			hs.Commit = walHS.Commit
		}
		if walHS.Term > hs.Term {
			hs.Term = walHS.Term
		}
		if walHS.Term == hs.Term && walHS.Vote != 0 {
			hs.Vote = walHS.Vote
		}
	}
	if hs.Commit > last {
		_ = db.Close()
		return nil, false, bin.ErrCorruptState
	}
	if hs.Commit < m.compacted {
		hs.Commit = m.compacted
	}

	lg := &raftLog{
		logDir:        logDir,
		db:            db,
		hs:            hs,
		cs:            m.cs,
		segs:          opened,
		prevSegs:      segFirsts(opened),
		tailID:        tailID,
		last:          last,
		hot:           newTailCache(cfg.LogTailSize),
		compacted:     m.compacted,
		compactedTerm: m.compactedTerm,
		nextID:        nextLogID(logDir, opened),
	}
	if err := lg.openTail(); err != nil {
		_ = db.Close()
		return nil, false, err
	}
	if err := lg.warmHotTail(); err != nil {
		_ = lg.Close()
		return nil, false, err
	}
	if hs.Commit != m.hs.Commit {
		lg.hs = hs
		if err := lg.storeMetaLocked(hs, lg.cs, true); err != nil {
			_ = lg.Close()
			return nil, false, err
		}
	}

	stateExists := last != 0 ||
		m.compacted != 0 ||
		!raft.IsEmptyHardState(m.hs) ||
		!emptyConfState(m.cs)
	return lg, stateExists, nil
}

func (r *raftLog) storeMetaLocked(
	hs raftpb.HardState, cs raftpb.ConfState, manifest bool,
) error {
	if !manifest &&
		termVoteEqual(r.hs, hs) &&
		confStateEqual(r.cs, cs) {
		return nil
	}

	return r.db.Update(func(tx *kvTx) error {
		mb := tx.Bucket(logMetaBucket)
		if !termVoteEqual(r.hs, hs) {
			if err := mb.Put(
				currentTermKey, bin.AppendUint64(nil, hs.Term),
			); err != nil {
				return err
			}
			if err := mb.Put(
				votedForKey, bin.AppendUint64(nil, hs.Vote),
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

		if err := mb.Put(
			commitKey, bin.AppendUint64(nil, hs.Commit),
		); err != nil {
			return err
		}

		sb := tx.Bucket(logSegBucket)
		cur := segFirsts(r.segs)

		for _, first := range r.prevSegs {
			if !containsU64(cur, first) {
				if err := sb.Delete(bin.AppendUint64(nil, first)); err != nil {
					return err
				}
			}
		}
		for _, seg := range r.segs {
			if !containsU64(r.prevSegs, seg.first) {
				key := bin.AppendUint64(nil, seg.first)
				value := bin.AppendUint64(nil, seg.id)
				if err := sb.Put(
					key,
					value,
				); err != nil {
					return err
				}
			}
		}
		r.prevSegs = cur

		compacted := make([]byte, 0, 16)
		compacted = bin.AppendUint64(compacted, r.compacted)
		compacted = bin.AppendUint64(compacted, r.compactedTerm)
		if err := mb.Put(compactedKey, compacted); err != nil {
			return err
		}
		return mb.Put(tailSegKey, bin.AppendUint64(nil, r.tailID))
	})
}

func loadRaftMeta(db *kvDB) (raftMeta, error) {
	var m raftMeta

	err := db.View(func(tx *kvTx) error {
		mb := tx.Bucket(logMetaBucket)
		sb := tx.Bucket(logSegBucket)

		if err := loadProto(mb, confStateKey, &m.cs); err != nil {
			return err
		}
		if v := mb.Get(currentTermKey); len(v) != 0 {
			term, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.hs.Term = term
		}
		if v := mb.Get(votedForKey); len(v) != 0 {
			vote, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.hs.Vote = vote
		}
		if v := mb.Get(commitKey); len(v) != 0 {
			commit, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.hs.Commit = commit
		}
		if v := mb.Get(tailSegKey); len(v) != 0 {
			id, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.tailID = id
		}
		if v := mb.Get(compactedKey); len(v) != 0 {
			idx, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			term, rest, err := bin.ReadUint64(rest)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.compacted = idx
			m.compactedTerm = term
		}

		c := sb.Cursor()
		defer func() { _ = c.Close() }()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			first, rest, err := bin.ReadUint64(k)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			id, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.segs = append(m.segs, logSeg{
				id:    id,
				first: first,
				last:  first - 1,
			})
		}
		return nil
	})
	return m, err
}

func putProto(b *kvBucket, key []byte, m protoMarshaler) error {
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	return b.Put(key, data)
}

func loadProto(b *kvBucket, key []byte, m protoUnmarshaler) error {
	data := b.Get(key)
	if len(data) == 0 {
		return nil
	}
	return m.Unmarshal(data)
}

func containsU64(s []uint64, v uint64) bool {
	return slices.Contains(s, v)
}

func confStateEqual(a, b raftpb.ConfState) bool {
	return a.Equivalent(b) == nil
}

func termVoteEqual(a, b raftpb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote
}

func emptyConfState(cs raftpb.ConfState) bool {
	return len(cs.Voters) == 0 &&
		len(cs.VotersOutgoing) == 0 &&
		len(cs.Learners) == 0 &&
		len(cs.LearnersNext) == 0 &&
		!cs.AutoLeave
}

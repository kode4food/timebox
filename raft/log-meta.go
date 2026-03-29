package raft

import (
	"os"
	"path/filepath"
	"slices"
	"time"

	"go.etcd.io/bbolt"
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
	walMetaFileName = "wal-meta.db"
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

	lg := &raftLog{
		logDir:        logDir,
		db:            db,
		hs:            hs,
		cs:            m.cs,
		segs:          opened,
		prevSegs:      segFirsts(opened),
		tailID:        tailID,
		last:          last,
		hot:           newTailCache(cfg.RecentEntriesSize),
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

	return r.db.Update(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(logMetaBucket)
		if !termVoteEqual(r.hs, hs) {
			if err := mb.Put(currentTermKey, putU64(nil, hs.Term)); err != nil {
				return err
			}
			if err := mb.Put(votedForKey, putU64(nil, hs.Vote)); err != nil {
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

		if err := mb.Put(commitKey, putU64(nil, hs.Commit)); err != nil {
			return err
		}

		sb := tx.Bucket(logSegBucket)
		cur := segFirsts(r.segs)

		for _, first := range r.prevSegs {
			if !containsU64(cur, first) {
				if err := sb.Delete(putU64(nil, first)); err != nil {
					return err
				}
			}
		}
		for _, seg := range r.segs {
			if !containsU64(r.prevSegs, seg.first) {
				if err := sb.Put(
					putU64(nil, seg.first),
					putU64(nil, seg.id),
				); err != nil {
					return err
				}
			}
		}
		r.prevSegs = cur

		if err := mb.Put(
			compactedKey,
			putCompacted(r.compacted, r.compactedTerm),
		); err != nil {
			return err
		}
		return mb.Put(tailSegKey, putU64(nil, r.tailID))
	})
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

func loadRaftMeta(db *bbolt.DB) (raftMeta, error) {
	var m raftMeta

	err := db.View(func(tx *bbolt.Tx) error {
		mb := tx.Bucket(logMetaBucket)
		sb := tx.Bucket(logSegBucket)

		if err := loadProto(mb, confStateKey, &m.cs); err != nil {
			return err
		}
		if v := mb.Get(currentTermKey); len(v) != 0 {
			term, err := getU64(v)
			if err != nil {
				return err
			}
			m.hs.Term = term
		}
		if v := mb.Get(votedForKey); len(v) != 0 {
			vote, err := getU64(v)
			if err != nil {
				return err
			}
			m.hs.Vote = vote
		}
		if v := mb.Get(commitKey); len(v) != 0 {
			commit, err := getU64(v)
			if err != nil {
				return err
			}
			m.hs.Commit = commit
		}
		if v := mb.Get(tailSegKey); len(v) != 0 {
			id, err := getU64(v)
			if err != nil {
				return err
			}
			m.tailID = id
		}
		if v := mb.Get(compactedKey); len(v) != 0 {
			idx, term, err := getCompacted(v)
			if err != nil {
				return err
			}
			m.compacted = idx
			m.compactedTerm = term
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
	return bin.AppendUint64(dst[:0], v)
}

func getU64(data []byte) (uint64, error) {
	v, rest, err := bin.ReadUint64(data)
	if err != nil {
		return 0, bin.ErrCorruptState
	}
	if len(rest) != 0 {
		return 0, bin.ErrCorruptState
	}
	return v, nil
}

func putCompacted(idx, term uint64) []byte {
	b := make([]byte, 0, 16)
	b = bin.AppendUint64(b, idx)
	b = bin.AppendUint64(b, term)
	return b
}

func getCompacted(data []byte) (uint64, uint64, error) {
	if len(data) == 0 {
		return 0, 0, nil
	}
	idx, rest, err := bin.ReadUint64(data)
	if err != nil {
		return 0, 0, bin.ErrCorruptState
	}
	term, rest, err := bin.ReadUint64(rest)
	if err != nil {
		return 0, 0, bin.ErrCorruptState
	}
	if len(rest) != 0 {
		return 0, 0, bin.ErrCorruptState
	}
	return idx, term, nil
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

package raft

import (
	"os"
	"path/filepath"
	"slices"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	bin "github.com/kode4food/timebox/internal/binary"
)

type raftMeta struct {
	segs          []logSeg
	cs            *raftpb.ConfState
	hs            *raftpb.HardState
	tailID        uint64
	compacted     uint64
	compactedTerm uint64
}

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

	hs := proto.Clone(m.hs).(*raftpb.HardState)
	commit := hs.GetCommit()
	term := hs.GetTerm()
	if walHS != nil {
		walCommit := walHS.GetCommit()
		walTerm := walHS.GetTerm()
		walVote := walHS.GetVote()
		if walCommit > commit {
			commit = walCommit
			hs.Commit = new(walCommit)
		}
		if walTerm > term {
			term = walTerm
			hs.Term = new(walTerm)
		}
		if walTerm == term && walVote != 0 {
			hs.Vote = new(walVote)
		}
	}
	if commit < m.compacted {
		commit = m.compacted
		hs.Commit = new(m.compacted)
	} else if commit > last {
		if last <= m.compacted {
			_ = db.Close()
			return nil, false, bin.ErrCorruptState
		}
		commit = last
		hs.Commit = new(last)
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
	if commit != m.hs.GetCommit() {
		lg.hs = hs
		if err := lg.storeMeta(hs, lg.cs, true); err != nil {
			_ = lg.Close()
			return nil, false, err
		}
	}

	stateExists := last != 0 || m.compacted != 0 ||
		!raft.IsEmptyHardState(m.hs) || !emptyConfState(m.cs)
	return lg, stateExists, nil
}

func (r *raftLog) storeMeta(
	hs *raftpb.HardState, cs *raftpb.ConfState, manifest bool,
) error {
	if !manifest && termVoteEqual(r.hs, hs) && confStateEqual(r.cs, cs) {
		return nil
	}

	return r.db.Update(func(tx *kvTx) error {
		mb := tx.Bucket(logMetaBucket)
		if !termVoteEqual(r.hs, hs) {
			if err := mb.Put(
				currentTermKey, bin.AppendUint64(nil, hs.GetTerm()),
			); err != nil {
				return err
			}
			if err := mb.Put(
				votedForKey, bin.AppendUint64(nil, hs.GetVote()),
			); err != nil {
				return err
			}
		}
		if !confStateEqual(r.cs, cs) {
			if err := putProto(mb, confStateKey, cs); err != nil {
				return err
			}
		}
		if !manifest {
			return nil
		}

		if err := mb.Put(
			commitKey, bin.AppendUint64(nil, hs.GetCommit()),
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
	m := raftMeta{
		cs: new(raftpb.ConfState),
		hs: new(raftpb.HardState),
	}

	err := db.View(func(tx *kvTx) error {
		mb := tx.Bucket(logMetaBucket)
		sb := tx.Bucket(logSegBucket)

		if err := loadProto(mb, confStateKey, m.cs); err != nil {
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
			m.hs.Term = new(term)
		}
		if v := mb.Get(votedForKey); len(v) != 0 {
			vote, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.hs.Vote = new(vote)
		}
		if v := mb.Get(commitKey); len(v) != 0 {
			commit, rest, err := bin.ReadUint64(v)
			if err != nil {
				return err
			}
			if len(rest) != 0 {
				return bin.ErrCorruptState
			}
			m.hs.Commit = new(commit)
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

func putProto(b *kvBucket, key []byte, m proto.Message) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}

func loadProto(b *kvBucket, key []byte, m proto.Message) error {
	data := b.Get(key)
	if len(data) == 0 {
		return nil
	}
	return proto.Unmarshal(data, m)
}

func containsU64(s []uint64, v uint64) bool {
	return slices.Contains(s, v)
}

func confStateEqual(a, b *raftpb.ConfState) bool {
	return a.Equivalent(b) == nil
}

func hardStateEqual(a, b *raftpb.HardState) bool {
	return termVoteEqual(a, b) && a.GetCommit() == b.GetCommit()
}

func termVoteEqual(a, b *raftpb.HardState) bool {
	return a.GetTerm() == b.GetTerm() && a.GetVote() == b.GetVote()
}

func emptyConfState(cs *raftpb.ConfState) bool {
	return len(cs.Voters) == 0 && len(cs.VotersOutgoing) == 0 &&
		len(cs.Learners) == 0 && len(cs.LearnersNext) == 0 &&
		!cs.GetAutoLeave()
}

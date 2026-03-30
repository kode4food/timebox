package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

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

func (p *Persistence) Ready() <-chan struct{} {
	return p.readyCh
}

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
	var buf bytes.Buffer
	var applied uint64
	err := p.db.View(func(tx *bbolt.Tx) error {
		var err error
		applied, err = loadLastAppliedTx(tx.Bucket(bucketName))
		if err != nil {
			return err
		}
		_, err = tx.WriteTo(&buf)
		return err
	})
	return buf.Bytes(), applied, err
}

func (p *Persistence) applySnapshot(snap raftpb.Snapshot) error {
	_ = p.db.Close()

	path := filepath.Join(p.DataDir, projectionDirName, projectionDBName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, snap.Data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}

	db, err := openBoltDB(path)
	if err != nil {
		return err
	}
	p.db = db
	p.fsm = newFSM(db, p.Config.Timebox.TrimEvents)
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

func openProjectionDB(dataDir string) (*bbolt.DB, error) {
	dir := filepath.Join(dataDir, projectionDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return openBoltDB(filepath.Join(dir, projectionDBName))
}

func rebuildProjection(p *Persistence, dataDir string, log *raftLog) error {
	_ = p.db.Close()
	path := filepath.Join(dataDir, projectionDirName, projectionDBName)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	db, err := openProjectionDB(dataDir)
	if err != nil {
		return err
	}
	p.db = db
	p.fsm = newFSM(db, p.Config.Timebox.TrimEvents)
	return p.restoreMaterializedState(log)
}

func openBoltDB(path string) (*bbolt.DB, error) {
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout:        time.Second,
		FreelistType:   bbolt.FreelistMapType,
		NoFreelistSync: true,
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

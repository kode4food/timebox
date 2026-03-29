package raft_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/bbolt"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/raft"
)

type (
	node struct {
		id          string
		addr        string
		persistence *raft.Persistence
		store       *timebox.Store
	}

	nodeConfig struct {
		id         string
		addr       string
		dataDir    string
		indexer    timebox.Indexer
		trimEvents bool
		publisher  raft.Publisher
	}
)

const testEventType timebox.EventType = "event.test"

func newNode(t *testing.T, cfg nodeConfig) *node {
	t.Helper()

	addr := cfg.addr
	if addr == "" {
		addr = freeAddr(t)
	}

	dataDir := cfg.dataDir
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	tbCfg := testRaftConfig(nodeConfig{
		id:         cfg.id,
		addr:       addr,
		dataDir:    dataDir,
		trimEvents: cfg.trimEvents,
		publisher:  cfg.publisher,
	})

	persistence, err := raft.NewPersistence(tbCfg)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	store, err := timebox.NewStore(persistence, tbCfg.Timebox)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	n := &node{
		id:          cfg.id,
		addr:        addr,
		persistence: persistence,
		store:       store,
	}
	t.Cleanup(func() {
		if n.store != nil {
			_ = n.store.Close()
		}
	})
	return n
}

func closeNode(t *testing.T, n *node) {
	t.Helper()

	if n == nil || n.store == nil {
		return
	}
	assert.NoError(t, n.store.Close())
	n.store = nil
}

func corruptMetaFile(t *testing.T, dataDir string) {
	t.Helper()

	db, err := bbolt.Open(
		filepath.Join(dataDir, "projection", "bolt.db"), 0o600, nil,
	)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte("timebox")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if bytes.HasSuffix(k, []byte("/meta")) {
				return tx.Bucket([]byte("timebox")).Put(
					k, []byte{0xff},
				)
			}
		}
		return assert.AnError
	})
	assert.NoError(t, err)
}

// decrementLastApplied rolls the projection's lastApplied cursor back by
// `by` raft log entries. A guard prevents going below index 3 so that
// conf-change entries (always at the very start of the log) are never
// included in the replay window
func decrementLastApplied(t *testing.T, dataDir string, by int64) {
	t.Helper()

	db, err := bbolt.Open(
		filepath.Join(dataDir, "projection", "bolt.db"), 0o600, nil,
	)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer func() {
		_ = db.Close()
	}()

	const minSafeIndex = int64(3) // indices 1-2 are conf-change/empty entries
	err = db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("timebox"))
		key := []byte("state/meta/last-applied-log")
		raw := b.Get(key)
		if len(raw) < 8 {
			return nil
		}
		current := int64(binary.BigEndian.Uint64(raw[:8]))
		target := current - by
		if target < minSafeIndex {
			return nil
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(target))
		return b.Put(key, buf[:])
	})
	assert.NoError(t, err)
}

func corruptLastApplied(t *testing.T, dataDir string) {
	t.Helper()

	db, err := bbolt.Open(
		filepath.Join(dataDir, "projection", "bolt.db"), 0o600, nil,
	)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte("timebox")).Put(
			[]byte("state/meta/last-applied-log"), []byte{0xff},
		)
	})
	assert.NoError(t, err)
}

func corruptRaftLogFile(t *testing.T, dataDir string) {
	t.Helper()

	files, err := filepath.Glob(
		filepath.Join(dataDir, "log", "*.log"),
	)
	if !assert.NoError(t, err) || !assert.NotEmpty(t, files) {
		t.FailNow()
	}
	assert.NoError(t, os.WriteFile(files[0], []byte("bad-raft-log"), 0o600))
}

func appendRaftTailGarbage(t *testing.T, dataDir string, data []byte) {
	t.Helper()

	files, err := filepath.Glob(
		filepath.Join(dataDir, "log", "*.log"),
	)
	if !assert.NoError(t, err) || !assert.NotEmpty(t, files) {
		t.FailNow()
	}

	path := files[len(files)-1]
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer func() {
		_ = f.Close()
	}()

	_, err = f.Write(data)
	assert.NoError(t, err)
}

func newCluster(t *testing.T, n int) []*node {
	t.Helper()

	srvs, cfgs := serverCfgs(t, n)
	return startNodes(t, cfgs, srvs)
}

func newClusterNode(t *testing.T, cfg nodeConfig, srvs []raft.Server) *node {
	t.Helper()

	dataDir := cfg.dataDir
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	tbCfg := testRaftConfig(nodeConfig{
		id:         cfg.id,
		addr:       cfg.addr,
		dataDir:    dataDir,
		trimEvents: cfg.trimEvents,
		publisher:  cfg.publisher,
	})
	tbCfg.Servers = srvs

	p, err := raft.NewPersistence(tbCfg)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	store, err := timebox.NewStore(p, tbCfg.Timebox)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	n := &node{
		id:          cfg.id,
		addr:        cfg.addr,
		persistence: p,
		store:       store,
	}
	t.Cleanup(func() {
		if n.store != nil {
			_ = n.store.Close()
		}
	})
	return n
}

func serverCfgs(t *testing.T, n int) ([]raft.Server, []nodeConfig) {
	t.Helper()

	srvs := make([]raft.Server, 0, n)
	cfgs := make([]nodeConfig, 0, n)
	for i := range n {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		srvs = append(srvs, raft.Server{
			ID:      id,
			Address: addr,
		})
		cfgs = append(cfgs, nodeConfig{
			id:      id,
			addr:    addr,
			dataDir: t.TempDir(),
		})
	}
	if !assert.Len(t, cfgs, n) {
		t.FailNow()
	}
	return srvs, cfgs
}

func startNodes(
	t *testing.T, cfgs []nodeConfig, srvs []raft.Server,
	idxs ...int,
) []*node {
	t.Helper()

	if len(idxs) == 0 {
		idxs = make([]int, len(cfgs))
		for i := range cfgs {
			idxs[i] = i
		}
	}

	nodes := make([]*node, 0, len(idxs))
	for _, idx := range idxs {
		n := newClusterNode(t, cfgs[idx], srvs)
		nodes = append(nodes, n)
	}
	return nodes
}

func waitReady(t *testing.T, n *node) {
	t.Helper()

	ctx, cancel := context.WithTimeout(
		context.Background(), 15*time.Second,
	)
	defer cancel()

	err := n.store.WaitReady(ctx)
	if err != nil {
		addr, leaderID := n.persistence.LeaderWithID()
		t.Logf(
			"node %s state=%s leader=%s/%s readyErr=%v",
			n.id,
			n.persistence.State(),
			leaderID,
			addr,
			err,
		)
	}
	if !assert.NoError(t, err) {
		t.FailNow()
	}
}

func waitReadyAll(t *testing.T, nodes []*node) {
	t.Helper()

	for _, n := range nodes {
		waitReady(t, n)
	}
}

func appendN(t *testing.T, s *timebox.Store, id timebox.AggregateID, n int) {
	t.Helper()

	for i := range n {
		err := s.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}
}

func waitEvents(t *testing.T, s *timebox.Store, id timebox.AggregateID, n int) {
	t.Helper()

	if !assert.Eventually(t, func() bool {
		evs, err := s.GetEvents(id, 0)
		return err == nil &&
			len(evs) == n &&
			evs[0].Sequence == 0 &&
			evs[n-1].Sequence == int64(n-1)
	}, 15*time.Second, 100*time.Millisecond) {
		t.FailNow()
	}
}

func waitAllEvents(
	t *testing.T, nodes []*node, id timebox.AggregateID, count int,
) {
	t.Helper()

	for _, n := range nodes {
		waitEvents(t, n.store, id, count)
	}
}

func waitForWrite(t *testing.T, store *timebox.Store) {
	t.Helper()

	if !assert.Eventually(t, func() bool {
		id := timebox.NewAggregateID(
			"probe",
			timebox.ID(fmt.Sprintf(
				"%d", time.Now().UnixNano(),
			)),
		)
		err := store.AppendEvents(
			id, 0, []*timebox.Event{numberEvent(id, 1)},
		)
		return err == nil
	}, 15*time.Second, 100*time.Millisecond) {
		t.FailNow()
	}
}

func testRaftConfig(cfg nodeConfig) raft.Config {
	indexer := combinedIndexer
	if cfg.indexer != nil {
		indexer = cfg.indexer
	}

	return raft.Config{
		LocalID: cfg.id,
		DataDir: cfg.dataDir,
		Address: cfg.addr,
		Timebox: timebox.Config{
			Indexer: indexer,
			Snapshot: timebox.SnapshotConfig{
				TrimEvents: cfg.trimEvents,
			},
		},
		Publisher: cfg.publisher,
	}
}

func findLeader(t *testing.T, nodes []*node) *node {
	t.Helper()

	var leader *node
	if !assert.Eventually(t, func() bool {
		for _, n := range nodes {
			if n.persistence.State() == raft.StateLeader {
				leader = n
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond) {
		t.FailNow()
	}
	return leader
}

func firstFollower(nodes []*node, leader *node) *node {
	for _, n := range nodes {
		if n.id != leader.id {
			return n
		}
	}
	return nil
}

func freeAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer func() { _ = ln.Close() }()

	return ln.Addr().String()
}

func numberEvent(id timebox.AggregateID, value int) *timebox.Event {
	return &timebox.Event{
		AggregateID: id,
		Timestamp:   time.Now().UTC(),
		Type:        testEventType,
		Data: json.RawMessage(
			fmt.Sprintf("%d", value),
		),
	}
}

func largeEvent(id timebox.AggregateID, value, size int) *timebox.Event {
	data := bytes.Repeat([]byte{'x'}, size)
	if len(data) != 0 {
		data[0] = byte('a' + value%26)
	}
	return &timebox.Event{
		AggregateID: id,
		Timestamp:   time.Now().UTC(),
		Type:        testEventType,
		Data:        data,
	}
}

func indexedEvent(
	id timebox.AggregateID, status, env string, ts time.Time,
) *timebox.Event {
	data := fmt.Sprintf(
		`{"status":%q,"env":%q}`, status, env,
	)
	return &timebox.Event{
		AggregateID: id,
		Timestamp:   ts,
		Type:        testEventType,
		Data:        json.RawMessage(data),
	}
}

func combinedIndexer(evs []*timebox.Event) []*timebox.Index {
	var idxs []*timebox.Index
	for _, e := range evs {
		data := map[string]string{}
		if err := json.Unmarshal(e.Data, &data); err != nil {
			continue
		}

		status := data["status"]
		idxs = append(idxs, &timebox.Index{
			Status: &status,
			Labels: map[string]string{
				"env": data["env"],
			},
		})
	}
	return idxs
}

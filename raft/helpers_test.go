package raft_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
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
		id             string
		addr           string
		dataDir        string
		trimEvents     bool
		compactMinStep uint64
		publisher      raft.Publisher
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
		id:             cfg.id,
		addr:           addr,
		dataDir:        dataDir,
		trimEvents:     cfg.trimEvents,
		compactMinStep: cfg.compactMinStep,
		publisher:      cfg.publisher,
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
		filepath.Join(dataDir, "bbolt.db"), 0o600, nil,
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

func corruptWALFile(t *testing.T, dataDir string) {
	t.Helper()

	matches, err := filepath.Glob(
		filepath.Join(dataDir, "raft-wal", "*.wal"),
	)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	if !assert.NotEmpty(t, matches) {
		t.FailNow()
	}
	assert.NoError(t,
		os.WriteFile(matches[0], []byte("bad-wal"), 0o600),
	)
}

func countSnapshotFiles(dir string) (int, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	n := 0
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		if strings.HasSuffix(ent.Name(), ".snap") {
			n++
		}
	}
	return n, nil
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
		id:             cfg.id,
		addr:           cfg.addr,
		dataDir:        dataDir,
		trimEvents:     cfg.trimEvents,
		compactMinStep: cfg.compactMinStep,
		publisher:      cfg.publisher,
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

func waitSnap(t *testing.T, dirs ...string) {
	t.Helper()

	if !assert.Eventually(t, func() bool {
		for _, dir := range dirs {
			n, err := countSnapshotFiles(dir)
			if err == nil && n != 0 {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond) {
		t.FailNow()
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
	return raft.Config{
		LocalID:        cfg.id,
		DataDir:        cfg.dataDir,
		Address:        cfg.addr,
		CompactMinStep: cfg.compactMinStep,
		Timebox: timebox.Config{
			Indexer: combinedIndexer,
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

package raft_test

import (
	"bytes"
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
		id         string
		addr       string
		dataDir    string
		trimEvents bool
	}
)

const testEventType timebox.EventType = "event.test"

func newNode(t *testing.T, cfg nodeConfig) *node {
	t.Helper()

	addr := cfg.addr
	if addr == "" {
		addr = freeAddr(t)
		if addr == "" {
			return nil
		}
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
	})

	persistence, err := raft.NewPersistence(tbCfg)
	if !assert.NoError(t, err) {
		return nil
	}

	store, err := timebox.NewStore(persistence, tbCfg.Timebox)
	if !assert.NoError(t, err) {
		return nil
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

	db, err := bbolt.Open(filepath.Join(dataDir, "bbolt.db"), 0o600, nil)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte("timebox")).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if bytes.HasSuffix(k, []byte("/meta")) {
				return tx.Bucket([]byte("timebox")).Put(k, []byte{0xff})
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
		return
	}
	if !assert.NotEmpty(t, matches) {
		return
	}
	assert.NoError(t, os.WriteFile(matches[0], []byte("bad-wal"), 0o600))
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

	nodes := make([]*node, 0, n)
	srvs := make([]raft.Server, 0, n)
	cfgs := make([]nodeConfig, 0, n)
	for i := range n {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		if !assert.NotEmpty(t, addr) {
			return nil
		}
		srvs = append(srvs, raft.Server{
			ID:      id,
			Address: addr,
		})
		cfgs = append(cfgs, nodeConfig{
			id:   id,
			addr: addr,
		})
	}

	for _, cfg := range cfgs {
		tbCfg := testRaftConfig(nodeConfig{
			id:      cfg.id,
			addr:    cfg.addr,
			dataDir: t.TempDir(),
		})
		tbCfg.Servers = srvs

		persistence, err := raft.NewPersistence(tbCfg)
		if !assert.NoError(t, err) {
			return nil
		}

		store, err := timebox.NewStore(persistence, tbCfg.Timebox)
		if !assert.NoError(t, err) {
			return nil
		}

		nn := &node{
			id:          cfg.id,
			addr:        cfg.addr,
			persistence: persistence,
			store:       store,
		}
		nodes = append(nodes, nn)
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			if n.store != nil {
				_ = n.store.Close()
			}
		}
	})
	return nodes
}

func newClusterNode(t *testing.T, cfg nodeConfig, srvs []raft.Server) *node {
	t.Helper()

	tbCfg := testRaftConfig(nodeConfig{
		id:      cfg.id,
		addr:    cfg.addr,
		dataDir: t.TempDir(),
	})
	tbCfg.Servers = srvs

	p, err := raft.NewPersistence(tbCfg)
	if !assert.NoError(t, err) {
		return nil
	}

	store, err := timebox.NewStore(p, tbCfg.Timebox)
	if !assert.NoError(t, err) {
		return nil
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

func waitForWrite(t *testing.T, store *timebox.Store) bool {
	t.Helper()

	ok := assert.Eventually(t, func() bool {
		id := timebox.NewAggregateID(
			"probe",
			timebox.ID(fmt.Sprintf("%d", time.Now().UnixNano())),
		)
		err := store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
		return err == nil
	}, 15*time.Second, 100*time.Millisecond)
	return ok
}

func testRaftConfig(cfg nodeConfig) raft.Config {
	return raft.Config{
		LocalID: cfg.id,
		DataDir: cfg.dataDir,
		Address: cfg.addr,
		Timebox: timebox.Config{
			Indexer: combinedIndexer,
			Snapshot: timebox.SnapshotConfig{
				TrimEvents: cfg.trimEvents,
			},
		},
	}
}

func findLeader(t *testing.T, nodes []*node) (*node, bool) {
	t.Helper()

	var leader *node
	ok := assert.Eventually(t, func() bool {
		for _, n := range nodes {
			if n.persistence.State() == raft.StateLeader {
				leader = n
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond)

	return leader, ok
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
		return ""
	}
	defer func() { _ = ln.Close() }()

	return ln.Addr().String()
}

func numberEvent(id timebox.AggregateID, value int) *timebox.Event {
	return &timebox.Event{
		AggregateID: id,
		Timestamp:   time.Now().UTC(),
		Type:        testEventType,
		Data:        json.RawMessage(fmt.Sprintf("%d", value)),
	}
}

func indexedEvent(
	id timebox.AggregateID, status, env string, ts time.Time,
) *timebox.Event {
	data := fmt.Sprintf(`{"status":%q,"env":%q}`, status, env)
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
			Labels: map[string]string{"env": data["env"]},
		})
	}
	return idxs
}

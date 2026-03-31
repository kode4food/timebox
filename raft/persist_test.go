package raft_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/raft"
)

func TestCorruptMeta(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "corrupt-meta")
	err := n.store.AppendEvents(id, 0, []*timebox.Event{
		numberEvent(id, 1),
	})
	if !assert.NoError(t, err) {
		return
	}

	closeNode(t, n)
	corruptMetaFile(t, cfg.dataDir)

	p, err := raft.NewPersistence(testRaftConfig(cfg))
	if !assert.NoError(t, err) {
		return
	}
	defer func() { _ = p.Close() }()

	_, err = p.GetAggregateStatus(id)
	assert.Error(t, err)
}

func TestCorruptBoltDB(t *testing.T) {
	dataDir := t.TempDir()

	projDir := filepath.Join(dataDir, "projection")
	if err := os.MkdirAll(projDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(projDir, "badger"), []byte("bad-path"), 0o600,
	); err != nil {
		t.Fatal(err)
	}

	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: dataDir,
	}
	_, err := raft.NewPersistence(testRaftConfig(cfg))
	assert.Error(t, err)
}

func TestBadRaftLogPath(t *testing.T) {
	dataDir := t.TempDir()

	raftLog := filepath.Join(dataDir, "log")
	if err := os.WriteFile(raftLog, []byte("bad-path"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: dataDir,
	}
	_, err := raft.NewPersistence(testRaftConfig(cfg))
	assert.Error(t, err)
}

func TestBusyRaftAddr(t *testing.T) {
	addr := freeAddr(t)

	ln, err := net.Listen("tcp", addr)
	if !assert.NoError(t, err) {
		return
	}
	defer func() {
		_ = ln.Close()
	}()

	cfg := nodeConfig{
		id:      "node-1",
		addr:    addr,
		dataDir: t.TempDir(),
	}
	_, err = raft.NewPersistence(testRaftConfig(cfg))
	assert.Error(t, err)
}

func TestCorruptRaftLog(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "corrupt-wal")
	err := n.store.AppendEvents(id, 0, []*timebox.Event{
		numberEvent(id, 1),
	})
	if !assert.NoError(t, err) {
		return
	}

	closeNode(t, n)
	corruptRaftLogFile(t, cfg.dataDir)

	_, err = raft.NewPersistence(testRaftConfig(cfg))
	assert.Error(t, err)
}

func TestBrokenRaftLog(t *testing.T) {
	const count = 256

	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "corrupt-raft-log")
	for i := range count {
		err := n.store.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	closeNode(t, n)
	corruptRaftLogFile(t, cfg.dataDir)

	_, err := raft.NewPersistence(testRaftConfig(cfg))
	assert.Error(t, err)
}

// TestRebuildProjection verifies that when the projection DB is corrupt
// (loadLastApplied fails), the node deletes and rebuilds it from the raft log
func TestRebuildProjection(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "rebuild")
	err := n.store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
	if !assert.NoError(t, err) {
		return
	}

	closeNode(t, n)
	corruptLastApplied(t, cfg.dataDir)

	n = newNode(t, cfg)
	waitForWrite(t, n.store)

	evs, err := n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, evs, 1)
}

// TestRestartReplays verifies that when the projection's lastApplied cursor
// falls behind the raft commit index (e.g. after a crash), the missing entries
// are replayed from the raft log on the next startup
func TestRestartReplays(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "replay-pending")
	appendN(t, n.store, id, 8)

	closeNode(t, n)
	decrementLastApplied(t, cfg.dataDir, 4)

	n = newNode(t, cfg)
	waitForWrite(t, n.store)

	evs, err := n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, evs, 8)
}

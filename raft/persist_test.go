package raft_test

import (
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/raft"
)

func TestAppend(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "append")
	firstTS := time.Unix(1_700_000_000, 0).UTC()
	secondTS := firstTS.Add(time.Minute)

	err := n.store.AppendEvents(id, 0, []*timebox.Event{
		indexedEvent(id, "active", "prod", firstTS),
		indexedEvent(id, "active", "prod", secondTS),
	})
	if !assert.NoError(t, err) {
		return
	}

	evs, err := n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, evs, 2)
	assert.Equal(t, int64(0), evs[0].Sequence)
	assert.Equal(t, int64(1), evs[1].Sequence)

	statuses, err := n.store.ListAggregatesByStatus("active")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        id,
		Timestamp: secondTS,
	}}, statuses)

	ids, err := n.store.ListAggregatesByLabel("env", "prod")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{id}, ids)

	err = n.store.AppendEvents(id, 1, []*timebox.Event{
		indexedEvent(id, "paused", "stage", secondTS.Add(time.Minute)),
	})
	if !assert.Error(t, err) {
		return
	}

	var conflict *timebox.VersionConflictError
	if !assert.ErrorAs(t, err, &conflict) {
		return
	}
	assert.Equal(t, int64(2), conflict.ActualSequence)
	assert.Len(t, conflict.NewEvents, 1)
	assert.Equal(t, int64(1), conflict.NewEvents[0].Sequence)
}

func TestSnapshot(t *testing.T) {
	n := newNode(t, nodeConfig{
		id:         "node-1",
		trimEvents: true,
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "snapshot")

	// Load snapshot before any data exists — aggregate unknown
	var emptyState map[string]int
	snap, err := n.store.GetSnapshot(id, &emptyState)
	if !assert.NoError(t, err) {
		return
	}
	assert.Nil(t, emptyState)
	assert.Equal(t, int64(0), snap.NextSequence)

	err = n.store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
	if !assert.NoError(t, err) {
		return
	}

	// Load snapshot before any snapshot stored — aggregate exists but no snap
	snap, err = n.store.GetSnapshot(id, &emptyState)
	if !assert.NoError(t, err) {
		return
	}
	assert.Nil(t, emptyState)
	assert.Equal(t, int64(0), snap.NextSequence)
	assert.Len(t, snap.AdditionalEvents, 1)

	err = n.store.PutSnapshot(id, map[string]int{"value": 1}, 1)
	if !assert.NoError(t, err) {
		return
	}

	// Stale snapshot — sequence lower than current snapshot; should be no-op
	err = n.store.PutSnapshot(id, map[string]int{"value": 0}, 0)
	if !assert.NoError(t, err) {
		return
	}

	err = n.store.AppendEvents(id, 1, []*timebox.Event{numberEvent(id, 2)})
	if !assert.NoError(t, err) {
		return
	}

	var snapState map[string]int
	snap, err = n.store.GetSnapshot(id, &snapState)
	if !assert.NoError(t, err) {
		return
	}
	// Stale snapshot was rejected; original seq-1 snapshot intact
	assert.Equal(t, map[string]int{"value": 1}, snapState)
	assert.Equal(t, int64(1), snap.NextSequence)
	assert.Len(t, snap.AdditionalEvents, 1)
	assert.Equal(t, int64(1), snap.AdditionalEvents[0].Sequence)

	evs, err := n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, evs, 1)
	assert.Equal(t, int64(1), evs[0].Sequence)
}

func TestSnapshotFutureSeq(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "future-snap")
	err := n.store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
	if !assert.NoError(t, err) {
		return
	}

	// Snapshot at seq 5 — ahead of current events (CurrentSequence=1)
	// exercises the cmd.Sequence > meta.CurrentSequence branch
	err = n.store.PutSnapshot(id, map[string]int{"value": 5}, 5)
	if !assert.NoError(t, err) {
		return
	}

	err = n.store.AppendEvents(id, 5, []*timebox.Event{numberEvent(id, 6)})
	if !assert.NoError(t, err) {
		return
	}

	var snapState map[string]int
	snap, err := n.store.GetSnapshot(id, &snapState)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, map[string]int{"value": 5}, snapState)
	assert.Equal(t, int64(5), snap.NextSequence)
	assert.Len(t, snap.AdditionalEvents, 1)
	assert.Equal(t, int64(5), snap.AdditionalEvents[0].Sequence)
}

func TestFollower(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := findLeader(t, nodes)
	follower := firstFollower(nodes, leader)
	if !assert.NotNil(t, follower) {
		return
	}
	assert.True(t, leader.persistence.CanSaveSnapshot())
	assert.Equal(t, leader.persistence.State(), raft.StateLeader)
	assert.False(t, follower.persistence.CanSaveSnapshot())
	assert.Equal(t, follower.persistence.State(), raft.StateFollower)

	id := timebox.NewAggregateID("order", "replicated")
	err := follower.store.AppendEvents(id, 0, []*timebox.Event{
		numberEvent(id, 1),
	})
	if !assert.NoError(t, err) {
		return
	}

	err = follower.store.PutSnapshot(id, map[string]int{"value": 1}, 1)
	if !assert.NoError(t, err) {
		return
	}

	err = follower.store.AppendEvents(id, 1, []*timebox.Event{
		numberEvent(id, 2),
	})
	if !assert.NoError(t, err) {
		return
	}

	assert.Eventually(t, func() bool {
		evs, err := leader.store.GetEvents(id, 0)
		if err != nil || len(evs) != 2 {
			return false
		}
		if evs[0].Sequence != 0 || evs[1].Sequence != 1 {
			return false
		}

		var snapState map[string]int
		snap, err := leader.store.GetSnapshot(id, &snapState)
		return err == nil &&
			snapState["value"] == 1 &&
			snap.NextSequence == 1 &&
			len(snap.AdditionalEvents) == 1 &&
			snap.AdditionalEvents[0].Sequence == 1
	}, 15*time.Second, 100*time.Millisecond)
}

func TestSingleReady(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	waitReady(t, n)

	assert.Equal(t, raft.StateLeader, n.persistence.State())
}

func TestFollowerStatus(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := findLeader(t, nodes)
	follower := firstFollower(nodes, leader)
	if !assert.NotNil(t, follower) {
		return
	}

	id := timebox.NewAggregateID("order", "forwarded-status")
	firstTS := time.Unix(1_700_000_000, 0).UTC()
	secondTS := firstTS.Add(time.Minute)

	err := follower.store.AppendEvents(id, 0, []*timebox.Event{
		indexedEvent(id, "active", "prod", firstTS),
	})
	if !assert.NoError(t, err) {
		return
	}

	status, err := follower.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "active", status)

	err = follower.store.AppendEvents(id, 1, []*timebox.Event{
		indexedEvent(id, "completed", "prod", secondTS),
	})
	if !assert.NoError(t, err) {
		return
	}

	status, err = follower.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "completed", status)
}

func TestCommittedPublisher(t *testing.T) {
	var mu sync.Mutex
	got := map[string][]*timebox.Event{}

	record := func(node string) raft.Publisher {
		return func(evs ...*timebox.Event) {
			mu.Lock()
			defer mu.Unlock()
			got[node] = append(got[node], evs...)
		}
	}

	srvs := []raft.Server{
		{ID: "node-1", Address: freeAddr(t)},
		{ID: "node-2", Address: freeAddr(t)},
		{ID: "node-3", Address: freeAddr(t)},
	}

	nodes := []*node{
		newClusterNode(t, nodeConfig{
			id:        "node-1",
			addr:      srvs[0].Address,
			publisher: record("node-1"),
		}, srvs),
		newClusterNode(t, nodeConfig{
			id:        "node-2",
			addr:      srvs[1].Address,
			publisher: record("node-2"),
		}, srvs),
		newClusterNode(t, nodeConfig{
			id:        "node-3",
			addr:      srvs[2].Address,
			publisher: record("node-3"),
		}, srvs),
	}
	leader := findLeader(t, nodes)
	follower := firstFollower(nodes, leader)
	if !assert.NotNil(t, follower) {
		return
	}

	id := timebox.NewAggregateID("order", "publisher")
	ts := time.Unix(1_700_000_000, 0).UTC()

	err := follower.store.AppendEvents(id, 0, []*timebox.Event{
		indexedEvent(id, "active", "prod", ts),
	})
	if !assert.NoError(t, err) {
		return
	}

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got["node-1"]) == 1 &&
			len(got["node-2"]) == 1 &&
			len(got["node-3"]) == 1
	}, 15*time.Second, 100*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, node := range []string{"node-1", "node-2", "node-3"} {
		evs := got[node]
		if assert.Len(t, evs, 1) {
			assert.Equal(t, id, evs[0].AggregateID)
			assert.Equal(t, int64(0), evs[0].Sequence)
			assert.Equal(t, testEventType, evs[0].Type)
		}
	}
}

func TestReadyCluster(t *testing.T) {
	nodes := newCluster(t, 3)
	waitReadyAll(t, nodes)
}

func TestDelayedJoin(t *testing.T) {
	srvs, cfgs := serverCfgs(t, 3)
	nodes := startNodes(t, cfgs, srvs, 1, 2)
	leader := findLeader(t, nodes)

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	nodes = append(nodes, late)

	id := timebox.NewAggregateID("order", "delayed-join")
	err := late.store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
	if !assert.NoError(t, err) {
		return
	}

	waitAllEvents(t, nodes, id, 1)

	status, err := leader.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "", status)
}

func TestDelayedJoinViaSnapshot(t *testing.T) {
	const count = 128

	srvs, cfgs := serverCfgs(t, 3)
	for i := range cfgs {
		cfgs[i].compactMinStep = 32
	}

	nodes := startNodes(t, cfgs, srvs, 1, 2)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "delayed-join-snapshot")
	appendN(t, leader.store, id, count)

	waitSnap(t,
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
		filepath.Join(cfgs[2].dataDir, "raft-snap"),
	)

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	nodes = append(nodes, late)
	waitAllEvents(t, nodes, id, count)
}

func TestSnapshotJoinPublish(t *testing.T) {
	const count = 128

	var mu sync.Mutex
	got := map[string][]*timebox.Event{}

	record := func(node string) raft.Publisher {
		return func(evs ...*timebox.Event) {
			mu.Lock()
			defer mu.Unlock()
			got[node] = append(got[node], evs...)
		}
	}

	srvs, cfgs := serverCfgs(t, 3)
	for i := range cfgs {
		cfgs[i].compactMinStep = 32
		cfgs[i].publisher = record(cfgs[i].id)
	}

	nodes := startNodes(t, cfgs, srvs, 1, 2)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "delayed-join-publisher")
	appendN(t, leader.store, id, count)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got["node-2"]) == count &&
			len(got["node-3"]) == count
	}, 15*time.Second, 100*time.Millisecond)

	waitSnap(t,
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
		filepath.Join(cfgs[2].dataDir, "raft-snap"),
	)

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	waitEvents(t, late.store, id, count)

	mu.Lock()
	assert.Empty(t, got["node-1"])
	mu.Unlock()

	snapDB := filepath.Join(cfgs[0].dataDir, "raft-snap", "*.snap.db")
	assert.Eventually(t, func() bool {
		matches, err := filepath.Glob(snapDB)
		return err == nil && len(matches) == 0
	}, 15*time.Second, 100*time.Millisecond)

	err := late.store.AppendEvents(id, int64(count), []*timebox.Event{
		numberEvent(id, count+1),
	})
	if !assert.NoError(t, err) {
		return
	}

	nodes = append(nodes, late)
	waitAllEvents(t, nodes, id, count+1)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got["node-1"]) == 1 &&
			len(got["node-2"]) == count+1 &&
			len(got["node-3"]) == count+1
	}, 15*time.Second, 100*time.Millisecond)

	mu.Lock()
	evs := got["node-1"]
	mu.Unlock()
	if assert.Len(t, evs, 1) {
		assert.Equal(t, id, evs[0].AggregateID)
		assert.Equal(t, int64(count), evs[0].Sequence)
	}
}

func TestSnapshotJoinRestart(t *testing.T) {
	const count = 128

	srvs, cfgs := serverCfgs(t, 3)
	for i := range cfgs {
		cfgs[i].compactMinStep = 32
	}

	nodes := startNodes(t, cfgs, srvs, 1, 2)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "restart-snapshot-join")
	appendN(t, leader.store, id, count)

	waitSnap(t,
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
		filepath.Join(cfgs[2].dataDir, "raft-snap"),
	)

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	waitEvents(t, late.store, id, count)

	closeNode(t, late)

	late = newClusterNode(t, cfgs[0], srvs)
	waitEvents(t, late.store, id, count)

	err := leader.store.AppendEvents(id, int64(count), []*timebox.Event{
		numberEvent(id, count+1),
	})
	if !assert.NoError(t, err) {
		return
	}

	waitReady(t, late)
	waitEvents(t, late.store, id, count+1)
}

func TestRestart(t *testing.T) {
	id := timebox.NewAggregateID("order", "restart")
	cfg := nodeConfig{
		id:         "node-1",
		addr:       freeAddr(t),
		dataDir:    t.TempDir(),
		trimEvents: true,
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	firstTS := time.Unix(1_700_000_000, 0).UTC()
	secondTS := firstTS.Add(time.Minute)

	err := n.store.AppendEvents(id, 0, []*timebox.Event{
		indexedEvent(id, "active", "prod", firstTS),
	})
	if !assert.NoError(t, err) {
		return
	}
	err = n.store.PutSnapshot(id, map[string]int{"value": 1}, 1)
	if !assert.NoError(t, err) {
		return
	}
	err = n.store.AppendEvents(id, 1, []*timebox.Event{
		indexedEvent(id, "paused", "stage", secondTS),
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NoError(t, n.store.Close()) {
		return
	}
	n.store = nil

	n = newNode(t, cfg)
	waitForWrite(t, n.store)

	var snapState map[string]int
	snap, err := n.store.GetSnapshot(id, &snapState)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, map[string]int{"value": 1}, snapState)
	assert.Equal(t, int64(1), snap.NextSequence)
	assert.Len(t, snap.AdditionalEvents, 1)
	assert.Equal(t, int64(1), snap.AdditionalEvents[0].Sequence)

	status, err := n.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "paused", status)

	statuses, err := n.store.ListAggregatesByStatus("paused")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        id,
		Timestamp: secondTS,
	}}, statuses)

	ids, err := n.store.ListAggregatesByLabel("env", "stage")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{id}, ids)

	vals, err := n.store.ListLabelValues("env")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"stage"}, vals)
}

func TestRestartCompaction(t *testing.T) {
	const count = 640

	dataDir := t.TempDir()
	cfg := nodeConfig{
		id:             "node-1",
		addr:           freeAddr(t),
		dataDir:        dataDir,
		compactMinStep: 512,
	}

	node := newNode(t, cfg)
	waitForWrite(t, node.store)

	id := timebox.NewAggregateID("order", "compacted-restart")
	for i := range count {
		err := node.store.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	snapDir := filepath.Join(dataDir, "raft-snap")
	assert.Eventually(t, func() bool {
		count, err := countSnapshotFiles(snapDir)
		return err == nil && count == 1
	}, 30*time.Second, 100*time.Millisecond)

	if !assert.NoError(t, node.store.Close()) {
		return
	}
	node.store = nil

	node = newNode(t, cfg)
	waitForWrite(t, node.store)

	evs, err := node.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, evs, count) {
		return
	}
	assert.Equal(t, int64(0), evs[0].Sequence)
	assert.Equal(t, int64(count-1), evs[count-1].Sequence)
}

func TestCompactionPrune(t *testing.T) {
	const count = 256

	cfg := nodeConfig{
		id:             "node-1",
		addr:           freeAddr(t),
		dataDir:        t.TempDir(),
		compactMinStep: 64,
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "prune")
	for i := range count {
		err := n.store.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	snapDir := filepath.Join(cfg.dataDir, "raft-snap")
	assert.Eventually(t, func() bool {
		n, err := countSnapshotFiles(snapDir)
		return err == nil && n == 1
	}, 30*time.Second, 100*time.Millisecond)
}

func TestLeaderRestart(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := findLeader(t, nodes)

	followers := make([]*node, 0, 2)
	for _, n := range nodes {
		if n != leader {
			followers = append(followers, n)
		}
	}
	if !assert.Len(t, followers, 2) {
		return
	}

	firstID := timebox.NewAggregateID("order", "restart-1")
	err := followers[0].store.AppendEvents(firstID, 0, []*timebox.Event{
		numberEvent(firstID, 1),
	})
	if !assert.NoError(t, err) {
		return
	}

	secondID := timebox.NewAggregateID("order", "restart-2")
	err = followers[1].store.AppendEvents(secondID, 0, []*timebox.Event{
		numberEvent(secondID, 1),
	})
	if !assert.NoError(t, err) {
		return
	}

	closeNode(t, leader)

	alive := []*node{followers[0], followers[1]}
	newLeader := findLeader(t, alive)

	var follower *node
	for _, n := range alive {
		if n != newLeader {
			follower = n
			break
		}
	}
	if !assert.NotNil(t, follower) {
		return
	}

	thirdID := timebox.NewAggregateID("order", "restart-3")
	err = follower.store.AppendEvents(thirdID, 0, []*timebox.Event{
		numberEvent(thirdID, 1),
	})
	if !assert.NoError(t, err) {
		return
	}

	assert.Eventually(t, func() bool {
		evs, err := newLeader.store.GetEvents(thirdID, 0)
		return err == nil && len(evs) == 1 && evs[0].Sequence == 0
	}, 15*time.Second, 100*time.Millisecond)
}

func TestFollowerRestartSnapshot(t *testing.T) {
	const count = 128

	srvs, cfgs := serverCfgs(t, 3)
	for i := range cfgs {
		cfgs[i].compactMinStep = 32
	}

	nodes := startNodes(t, cfgs, srvs)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	var down *node
	var downCfg nodeConfig
	alive := make([]*node, 0, 2)
	for i, n := range nodes {
		if n.id == leader.id {
			continue
		}
		down = n
		downCfg = cfgs[i]
		break
	}
	if !assert.NotNil(t, down) {
		return
	}
	for _, n := range nodes {
		if n.id != down.id {
			alive = append(alive, n)
		}
	}

	closeNode(t, down)

	id := timebox.NewAggregateID("order", "restart-follower-snapshot")
	appendN(t, leader.store, id, count)

	waitSnap(t,
		filepath.Join(cfgs[0].dataDir, "raft-snap"),
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
		filepath.Join(cfgs[2].dataDir, "raft-snap"),
	)

	down = newClusterNode(t, downCfg, srvs)
	waitReady(t, down)

	nodes = append(alive, down)
	waitAllEvents(t, nodes, id, count)

	err := down.store.AppendEvents(id, int64(count), []*timebox.Event{
		numberEvent(id, count+1),
	})
	if !assert.NoError(t, err) {
		return
	}

	waitAllEvents(t, nodes, id, count+1)
}

func TestDeadPeerSnapshot(t *testing.T) {
	const count = 128

	fakeAddr := freeAddr(t)

	ln, err := net.Listen("tcp", fakeAddr)
	if !assert.NoError(t, err) {
		return
	}
	t.Cleanup(func() {
		_ = ln.Close()
	})

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	srvs := []raft.Server{
		{ID: "node-1", Address: freeAddr(t)},
		{ID: "node-2", Address: freeAddr(t)},
		{ID: "node-3", Address: fakeAddr},
	}

	cfgs := []nodeConfig{
		{
			id:             "node-1",
			addr:           srvs[0].Address,
			dataDir:        t.TempDir(),
			compactMinStep: 32,
		},
		{
			id:             "node-2",
			addr:           srvs[1].Address,
			dataDir:        t.TempDir(),
			compactMinStep: 32,
		},
	}

	n1 := newClusterNode(t, cfgs[0], srvs)
	n2 := newClusterNode(t, cfgs[1], srvs)
	if n1 == nil || n2 == nil {
		return
	}

	nodes := []*node{n1, n2}
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "dead-peer-snapshot")
	appendN(t, leader.store, id, count)

	waitSnap(t,
		filepath.Join(cfgs[0].dataDir, "raft-snap"),
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
	)

	waitAllEvents(t, nodes, id, count)
}

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

	// Use a directory where bbolt.db file should go — bbolt.Open will fail
	if err := os.Mkdir(filepath.Join(dataDir, "bbolt.db"), 0o755); err != nil {
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

func TestBadSnapDir(t *testing.T) {
	dataDir := t.TempDir()

	snapDir := filepath.Join(dataDir, "raft-snap")
	if err := os.WriteFile(snapDir, []byte("bad-snap-dir"), 0o600); err != nil {
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

func TestCorruptWAL(t *testing.T) {
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
	corruptWALFile(t, cfg.dataDir)

	_, err = raft.NewPersistence(testRaftConfig(cfg))
	assert.Error(t, err)
}

func TestAppendClosed(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	p := n.persistence
	closeNode(t, n)

	id := timebox.NewAggregateID("order", "append-closed")
	err := p.Append(timebox.AppendRequest{
		ID:               id,
		ExpectedSequence: 0,
		Events:           []*timebox.Event{numberEvent(id, 1)},
	})
	assert.Error(t, err)
}

func TestBrokenRaftSnapshot(t *testing.T) {
	const count = 256

	cfg := nodeConfig{
		id:             "node-1",
		addr:           freeAddr(t),
		dataDir:        t.TempDir(),
		compactMinStep: 64,
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "corrupt-raft-snapshot")
	for i := range count {
		err := n.store.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	snapDir := filepath.Join(cfg.dataDir, "raft-snap")
	var snapPath string
	assert.Eventually(t, func() bool {
		matches, err := filepath.Glob(filepath.Join(snapDir, "*.snap"))
		if err != nil || len(matches) == 0 {
			return false
		}
		snapPath = matches[0]
		return true
	}, 30*time.Second, 100*time.Millisecond)

	closeNode(t, n)
	assert.NoError(t, os.WriteFile(snapPath, []byte("bad-snap"), 0o600))

	p, err := raft.NewPersistence(testRaftConfig(cfg))
	if !assert.NoError(t, err) {
		return
	}
	defer func() { _ = p.Close() }()

	res, err := p.LoadEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, res.Events, count) {
		return
	}
	assert.Equal(t, int64(0), res.Events[0].Sequence)
	assert.Equal(t, int64(count-1), res.Events[count-1].Sequence)
}

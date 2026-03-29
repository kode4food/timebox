package raft_test

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/raft"
)

// TestSnapshotTransfer verifies that when a late-joining node needs entries
// that have been compacted away, the leader sends a full DB snapshot and the
// follower applies it and catches up correctly
func TestSnapshotTransfer(t *testing.T) {
	const count = 16

	orig := *raft.LogRotateBytesPtr
	*raft.LogRotateBytesPtr = 1
	defer func() { *raft.LogRotateBytesPtr = orig }()

	srvs, cfgs := serverCfgs(t, 3)
	nodes := startNodes(t, cfgs, srvs, 1, 2)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "snapshot-transfer")
	appendN(t, leader.store, id, count)
	waitAllEvents(t, nodes, id, count)

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	waitEvents(t, late.store, id, count)
}

func TestFollower(t *testing.T) {
	nodes := newCluster(t, 3)
	leader := findLeader(t, nodes)
	follower := firstFollower(nodes, leader)
	if !assert.NotNil(t, follower) {
		return
	}
	assert.False(t, leader.persistence.CanSaveSnapshot())
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

func TestDelayedJoinViaLog(t *testing.T) {
	const count = 128

	srvs, cfgs := serverCfgs(t, 3)
	nodes := startNodes(t, cfgs, srvs, 1, 2)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "delayed-join-log")
	appendN(t, leader.store, id, count)

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	nodes = append(nodes, late)
	waitAllEvents(t, nodes, id, count)
}

func TestDelayedJoinLargeLogPages(t *testing.T) {
	const (
		count       = 24
		payloadSize = 96 * 1024
	)

	srvs, cfgs := serverCfgs(t, 3)
	nodes := startNodes(t, cfgs, srvs, 1, 2)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "delayed-join-large-pages")
	for i := range count {
		err := leader.store.AppendEvents(id, int64(i), []*timebox.Event{
			largeEvent(id, i+1, payloadSize),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	nodes = append(nodes, late)
	waitAllEvents(t, nodes, id, count)
}

func TestLogJoinPublish(t *testing.T) {
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

	late := newClusterNode(t, cfgs[0], srvs)
	waitReady(t, late)
	waitEvents(t, late.store, id, count)

	mu.Lock()
	assert.Empty(t, got["node-1"])
	mu.Unlock()

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

func TestLogJoinRestart(t *testing.T) {
	const count = 128

	srvs, cfgs := serverCfgs(t, 3)
	nodes := startNodes(t, cfgs, srvs, 1, 2)
	waitReadyAll(t, nodes)
	leader := findLeader(t, nodes)

	id := timebox.NewAggregateID("order", "restart-log-join")
	appendN(t, leader.store, id, count)

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

func TestRestartRetainedLog(t *testing.T) {
	const count = 640

	dataDir := t.TempDir()
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: dataDir,
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

func TestFollowerRestartCatchUp(t *testing.T) {
	const count = 128

	srvs, cfgs := serverCfgs(t, 3)
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

	id := timebox.NewAggregateID("order", "restart-follower-log")
	appendN(t, leader.store, id, count)

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

func TestDeadPeerCatchUp(t *testing.T) {
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
			id:      "node-1",
			addr:    srvs[0].Address,
			dataDir: t.TempDir(),
		},
		{
			id:      "node-2",
			addr:    srvs[1].Address,
			dataDir: t.TempDir(),
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

	waitAllEvents(t, nodes, id, count)
}

func TestRestartRepairsTailGarbage(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	waitForWrite(t, n.store)

	id := timebox.NewAggregateID("order", "repair-tail")
	appendN(t, n.store, id, 8)

	closeNode(t, n)
	appendRaftTailGarbage(t, cfg.dataDir, []byte{0xde, 0xad, 0xbe, 0xef})

	n = newNode(t, cfg)
	waitForWrite(t, n.store)

	evs, err := n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, evs, 8) {
		return
	}
	assert.Equal(t, int64(0), evs[0].Sequence)
	assert.Equal(t, int64(7), evs[7].Sequence)

	err = n.store.AppendEvents(id, 8, []*timebox.Event{
		numberEvent(id, 9),
	})
	if !assert.NoError(t, err) {
		return
	}

	evs, err = n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, evs, 9)
}

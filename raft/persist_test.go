package raft_test

import (
	"context"
	"fmt"
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
	if n == nil {
		return
	}

	if !waitForWrite(t, n.store) {
		return
	}

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
	if n == nil {
		return
	}

	if !waitForWrite(t, n.store) {
		return
	}

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
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

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
	if len(nodes) != 3 {
		return
	}

	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}
	follower := firstFollower(nodes, leader)
	if !assert.NotNil(t, follower) {
		return
	}
	assert.True(t, leader.persistence.CanSaveSnapshot())
	assert.Equal(t, leader.persistence.State(), raft.StateLeader)
	assert.False(t, follower.persistence.CanSaveSnapshot())
	assert.Equal(t, follower.persistence.State(), raft.StateFollower)

	id := timebox.NewAggregateID("order", "replicated")
	err := follower.store.AppendEvents(
		id,
		0,
		[]*timebox.Event{numberEvent(id, 1)},
	)
	if !assert.NoError(t, err) {
		return
	}

	err = follower.store.PutSnapshot(id, map[string]int{"value": 1}, 1)
	if !assert.NoError(t, err) {
		return
	}

	err = follower.store.AppendEvents(
		id,
		1,
		[]*timebox.Event{numberEvent(id, 2)},
	)
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

func TestIsLeaderSingle(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	if n == nil {
		return
	}

	if !waitForWrite(t, n.store) {
		return
	}

	assert.Equal(t, n.persistence.State(), raft.StateLeader)
}

func TestFollowerStatus(t *testing.T) {
	nodes := newCluster(t, 3)
	if len(nodes) != 3 {
		return
	}

	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}
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
	if len(nodes) != 3 {
		return
	}

	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}
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

func TestReadySingle(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	if n == nil {
		return
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	assert.NoError(t, n.store.WaitReady(ctx))
}

func TestReadyCluster(t *testing.T) {
	nodes := newCluster(t, 3)
	if len(nodes) != 3 {
		return
	}

	for _, n := range nodes {
		ctx, cancel := context.WithTimeout(
			context.Background(), 15*time.Second,
		)
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
		assert.NoError(t, err)
		cancel()
	}
}

func TestDelayedJoin(t *testing.T) {
	srvs := make([]raft.Server, 0, 3)
	cfgs := make([]nodeConfig, 0, 3)
	for i := range 3 {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		if !assert.NotEmpty(t, addr) {
			return
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

	n2 := newClusterNode(t, cfgs[1], srvs)
	n3 := newClusterNode(t, cfgs[2], srvs)
	if n2 == nil || n3 == nil {
		return
	}

	nodes := []*node{n2, n3}
	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}

	n1 := newClusterNode(t, cfgs[0], srvs)
	if n1 == nil {
		return
	}
	nodes = append(nodes, n1)

	for _, n := range nodes {
		ctx, cancel := context.WithTimeout(
			context.Background(), 15*time.Second,
		)
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
		assert.NoError(t, err)
		cancel()
	}

	id := timebox.NewAggregateID("order", "delayed-join")
	err := leader.store.AppendEvents(
		id,
		0,
		[]*timebox.Event{numberEvent(id, 1)},
	)
	if !assert.NoError(t, err) {
		return
	}

	for _, n := range nodes {
		assert.Eventually(t, func() bool {
			evs, err := n.store.GetEvents(id, 0)
			return err == nil &&
				len(evs) == 1 &&
				evs[0].Sequence == 0
		}, 15*time.Second, 100*time.Millisecond)
	}
}

func TestDelayedJoinFollowerReady(t *testing.T) {
	srvs := make([]raft.Server, 0, 3)
	cfgs := make([]nodeConfig, 0, 3)
	for i := range 3 {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		if !assert.NotEmpty(t, addr) {
			return
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

	n2 := newClusterNode(t, cfgs[1], srvs)
	n3 := newClusterNode(t, cfgs[2], srvs)
	if n2 == nil || n3 == nil {
		return
	}

	nodes := []*node{n2, n3}
	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}

	n1 := newClusterNode(t, cfgs[0], srvs)
	if n1 == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	assert.NoError(t, n1.store.WaitReady(ctx))

	id := timebox.NewAggregateID("order", "delayed-join-ready")
	err := n1.store.AppendEvents(id, 0, []*timebox.Event{
		numberEvent(id, 1),
	})
	if !assert.NoError(t, err) {
		return
	}

	for _, n := range append(nodes, n1) {
		assert.Eventually(t, func() bool {
			evs, err := n.store.GetEvents(id, 0)
			return err == nil &&
				len(evs) == 1 &&
				evs[0].Sequence == 0
		}, 15*time.Second, 100*time.Millisecond)
	}

	status, err := leader.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "", status)
}

func TestDelayedJoinViaSnapshot(t *testing.T) {
	const count = 128

	srvs := make([]raft.Server, 0, 3)
	cfgs := make([]nodeConfig, 0, 3)
	for i := range 3 {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		if !assert.NotEmpty(t, addr) {
			return
		}
		srvs = append(srvs, raft.Server{
			ID:      id,
			Address: addr,
		})
		cfgs = append(cfgs, nodeConfig{
			id:             id,
			addr:           addr,
			dataDir:        t.TempDir(),
			compactMinStep: 32,
		})
	}

	n2 := newClusterNode(t, cfgs[1], srvs)
	n3 := newClusterNode(t, cfgs[2], srvs)
	if n2 == nil || n3 == nil {
		return
	}

	nodes := []*node{n2, n3}
	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}

	id := timebox.NewAggregateID("order", "delayed-join-snapshot")
	for i := range count {
		err := leader.store.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	snapDirs := []string{
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
		filepath.Join(cfgs[2].dataDir, "raft-snap"),
	}
	assert.Eventually(t, func() bool {
		for _, dir := range snapDirs {
			n, err := countSnapshotFiles(dir)
			if err == nil && n != 0 {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	n1 := newClusterNode(t, cfgs[0], srvs)
	if n1 == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	assert.NoError(t, n1.store.WaitReady(ctx))

	for _, n := range append(nodes, n1) {
		assert.Eventually(t, func() bool {
			evs, err := n.store.GetEvents(id, 0)
			return err == nil &&
				len(evs) == count &&
				evs[0].Sequence == 0 &&
				evs[count-1].Sequence == int64(count-1)
		}, 15*time.Second, 100*time.Millisecond)
	}
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

	srvs := make([]raft.Server, 0, 3)
	cfgs := make([]nodeConfig, 0, 3)
	for i := range 3 {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		if !assert.NotEmpty(t, addr) {
			return
		}
		srvs = append(srvs, raft.Server{
			ID:      id,
			Address: addr,
		})
		cfgs = append(cfgs, nodeConfig{
			id:             id,
			addr:           addr,
			dataDir:        t.TempDir(),
			compactMinStep: 32,
			publisher:      record(id),
		})
	}

	n2 := newClusterNode(t, cfgs[1], srvs)
	n3 := newClusterNode(t, cfgs[2], srvs)
	if n2 == nil || n3 == nil {
		return
	}

	nodes := []*node{n2, n3}
	for _, n := range nodes {
		ctx, cancel := context.WithTimeout(
			context.Background(), 15*time.Second,
		)
		err := n.store.WaitReady(ctx)
		cancel()
		if !assert.NoError(t, err) {
			return
		}
	}

	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}

	id := timebox.NewAggregateID("order", "delayed-join-publisher")
	for i := range count {
		err := leader.store.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got["node-2"]) == count &&
			len(got["node-3"]) == count
	}, 15*time.Second, 100*time.Millisecond)

	snapDirs := []string{
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
		filepath.Join(cfgs[2].dataDir, "raft-snap"),
	}
	assert.Eventually(t, func() bool {
		for _, dir := range snapDirs {
			n, err := countSnapshotFiles(dir)
			if err == nil && n != 0 {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	n1 := newClusterNode(t, cfgs[0], srvs)
	if n1 == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := n1.store.WaitReady(ctx)
	cancel()
	if !assert.NoError(t, err) {
		return
	}

	assert.Eventually(t, func() bool {
		evs, err := n1.store.GetEvents(id, 0)
		return err == nil &&
			len(evs) == count &&
			evs[count-1].Sequence == int64(count-1)
	}, 15*time.Second, 100*time.Millisecond)

	mu.Lock()
	assert.Empty(t, got["node-1"])
	mu.Unlock()

	snapDB := filepath.Join(cfgs[0].dataDir, "raft-snap", "*.snap.db")
	assert.Eventually(t, func() bool {
		matches, err := filepath.Glob(snapDB)
		return err == nil && len(matches) == 0
	}, 15*time.Second, 100*time.Millisecond)

	err = n1.store.AppendEvents(id, int64(count), []*timebox.Event{
		numberEvent(id, count+1),
	})
	if !assert.NoError(t, err) {
		return
	}

	for _, n := range append(nodes, n1) {
		assert.Eventually(t, func() bool {
			evs, err := n.store.GetEvents(id, 0)
			return err == nil &&
				len(evs) == count+1 &&
				evs[count].Sequence == int64(count)
		}, 15*time.Second, 100*time.Millisecond)
	}

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

	srvs := make([]raft.Server, 0, 3)
	cfgs := make([]nodeConfig, 0, 3)
	for i := range 3 {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		if !assert.NotEmpty(t, addr) {
			return
		}
		srvs = append(srvs, raft.Server{
			ID:      id,
			Address: addr,
		})
		cfgs = append(cfgs, nodeConfig{
			id:             id,
			addr:           addr,
			dataDir:        t.TempDir(),
			compactMinStep: 32,
		})
	}

	n2 := newClusterNode(t, cfgs[1], srvs)
	n3 := newClusterNode(t, cfgs[2], srvs)
	if n2 == nil || n3 == nil {
		return
	}

	nodes := []*node{n2, n3}
	for _, n := range nodes {
		ctx, cancel := context.WithTimeout(
			context.Background(), 15*time.Second,
		)
		err := n.store.WaitReady(ctx)
		cancel()
		if !assert.NoError(t, err) {
			return
		}
	}

	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}

	id := timebox.NewAggregateID("order", "restart-snapshot-join")
	for i := range count {
		err := leader.store.AppendEvents(id, int64(i), []*timebox.Event{
			numberEvent(id, i+1),
		})
		if !assert.NoError(t, err) {
			return
		}
	}

	snapDirs := []string{
		filepath.Join(cfgs[1].dataDir, "raft-snap"),
		filepath.Join(cfgs[2].dataDir, "raft-snap"),
	}
	assert.Eventually(t, func() bool {
		for _, dir := range snapDirs {
			n, err := countSnapshotFiles(dir)
			if err == nil && n != 0 {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	n1 := newClusterNode(t, cfgs[0], srvs)
	if n1 == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := n1.store.WaitReady(ctx)
	cancel()
	if !assert.NoError(t, err) {
		return
	}

	assert.Eventually(t, func() bool {
		evs, err := n1.store.GetEvents(id, 0)
		return err == nil &&
			len(evs) == count &&
			evs[count-1].Sequence == int64(count-1)
	}, 15*time.Second, 100*time.Millisecond)

	closeNode(t, n1)

	n1 = newClusterNode(t, cfgs[0], srvs)
	if n1 == nil {
		return
	}

	assert.Eventually(t, func() bool {
		evs, err := n1.store.GetEvents(id, 0)
		return err == nil &&
			len(evs) == count &&
			evs[0].Sequence == 0 &&
			evs[count-1].Sequence == int64(count-1)
	}, 15*time.Second, 100*time.Millisecond)

	err = leader.store.AppendEvents(id, int64(count), []*timebox.Event{
		numberEvent(id, count+1),
	})
	if !assert.NoError(t, err) {
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	err = n1.store.WaitReady(ctx)
	cancel()
	if !assert.NoError(t, err) {
		return
	}

	assert.Eventually(t, func() bool {
		evs, err := n1.store.GetEvents(id, 0)
		return err == nil &&
			len(evs) == count+1 &&
			evs[0].Sequence == 0 &&
			evs[count].Sequence == int64(count)
	}, 15*time.Second, 100*time.Millisecond)
}

func TestRestart(t *testing.T) {
	addr := freeAddr(t)
	if addr == "" {
		return
	}

	id := timebox.NewAggregateID("order", "restart")
	cfg := nodeConfig{
		id:         "node-1",
		addr:       addr,
		dataDir:    t.TempDir(),
		trimEvents: true,
	}

	n := newNode(t, cfg)
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

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
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

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

	addr := freeAddr(t)
	if addr == "" {
		return
	}

	dataDir := t.TempDir()
	cfg := nodeConfig{
		id:             "node-1",
		addr:           addr,
		dataDir:        dataDir,
		compactMinStep: 512,
	}

	node := newNode(t, cfg)
	if node == nil {
		return
	}
	if !waitForWrite(t, node.store) {
		return
	}

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
	if node == nil {
		return
	}
	if !waitForWrite(t, node.store) {
		return
	}

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
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

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
	if len(nodes) != 3 {
		return
	}

	leader, ok := findLeader(t, nodes)
	if !ok {
		return
	}

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
	newLeader, ok := findLeader(t, alive)
	if !ok {
		return
	}

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

func TestCorruptMeta(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

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

func TestCorruptWAL(t *testing.T) {
	cfg := nodeConfig{
		id:      "node-1",
		addr:    freeAddr(t),
		dataDir: t.TempDir(),
	}

	n := newNode(t, cfg)
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

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

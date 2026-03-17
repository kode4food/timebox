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
		id         string
		addr       string
		dataDir    string
		trimEvents bool
	}
)

const testEventType timebox.EventType = "event.test"

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

func TestSnapshotContext(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := n.persistence.SaveSnapshot(
		ctx, timebox.NewAggregateID("order", "ctx"), []byte(`{}`), 0,
	)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSnapshotShortDeadline(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

	id := timebox.NewAggregateID("order", "short-deadline")
	err := n.store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
	if !assert.NoError(t, err) {
		return
	}

	// Use a deadline shorter than the default applyTimeout (10s) to exercise
	// the commandTimeout deadline branch.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = n.persistence.SaveSnapshot(ctx, id, []byte(`{}`), 1)
	assert.NoError(t, err)
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
	assert.False(t, follower.persistence.CanSaveSnapshot())

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

func TestReadySingle(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	if n == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	t.Setenv("TIMEBOX_RAFT_TEST_COMPACT_MIN_STEP", "512")

	addr := freeAddr(t)
	if addr == "" {
		return
	}

	dataDir := t.TempDir()
	cfg := nodeConfig{
		id:      "node-1",
		addr:    addr,
		dataDir: dataDir,
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

	t.Setenv("TIMEBOX_RAFT_TEST_COMPACT_MIN_STEP", "64")

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

func TestIndexes(t *testing.T) {
	n := newNode(t, nodeConfig{
		id: "node-1",
	})
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

	id := timebox.NewAggregateID("order", "indexes")
	base := time.Unix(1_700_000_000, 0).UTC()

	err := n.store.AppendEvents(id, 0, []*timebox.Event{
		indexedEvent(id, "active", "prod", base),
	})
	if !assert.NoError(t, err) {
		return
	}

	err = n.store.AppendEvents(id, 1, []*timebox.Event{
		indexedEvent(id, "paused", "stage", base.Add(time.Minute)),
	})
	if !assert.NoError(t, err) {
		return
	}

	status, err := n.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "paused", status)

	active, err := n.store.ListAggregatesByStatus("active")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, active)

	paused, err := n.store.ListAggregatesByStatus("paused")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        id,
		Timestamp: base.Add(time.Minute),
	}}, paused)

	prod, err := n.store.ListAggregatesByLabel("env", "prod")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, prod)

	stage, err := n.store.ListAggregatesByLabel("env", "stage")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{id}, stage)

	vals, err := n.store.ListLabelValues("env")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"stage"}, vals)

	err = n.store.AppendEvents(id, 2, []*timebox.Event{
		indexedEvent(id, "", "", base.Add(2*time.Minute)),
	})
	if !assert.NoError(t, err) {
		return
	}

	status, err = n.store.GetAggregateStatus(id)
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, status)

	paused, err = n.store.ListAggregatesByStatus("paused")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, paused)

	stage, err = n.store.ListAggregatesByLabel("env", "stage")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, stage)

	vals, err = n.store.ListLabelValues("env")
	if !assert.NoError(t, err) {
		return
	}
	assert.Empty(t, vals)
}

func TestQueries(t *testing.T) {
	for _, trimEvents := range []bool{false, true} {
		name := "untrimmed"
		if trimEvents {
			name = "trimmed"
		}

		t.Run(name, func(t *testing.T) {
			n := newNode(t, nodeConfig{
				id:         "node-1",
				trimEvents: trimEvents,
			})
			if n == nil {
				return
			}
			if !waitForWrite(t, n.store) {
				return
			}

			first := timebox.NewAggregateID("order", "1")
			second := timebox.NewAggregateID("order", "2")
			third := timebox.NewAggregateID("invoice", "1")
			base := time.Unix(1_700_000_000, 0).UTC()

			assert.NoError(t, n.store.AppendEvents(first, 0, []*timebox.Event{
				indexedEvent(first, "active", "prod", base),
			}))
			assert.NoError(t, n.store.AppendEvents(second, 0, []*timebox.Event{
				indexedEvent(second, "active", "prod", base.Add(time.Minute)),
			}))
			assert.NoError(t, n.store.AppendEvents(third, 0, []*timebox.Event{
				indexedEvent(third, "paused", "stage", base.Add(2*time.Minute)),
			}))

			ids, err := n.store.ListAggregates(timebox.NewAggregateID("order"))
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []timebox.AggregateID{first, second}, ids)

			status, err := n.store.GetAggregateStatus(first)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, "active", status)

			status, err = n.store.GetAggregateStatus(
				timebox.NewAggregateID("missing", "1"),
			)
			if !assert.NoError(t, err) {
				return
			}
			assert.Empty(t, status)

			active, err := n.store.ListAggregatesByStatus("active")
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []timebox.StatusEntry{
				{ID: first, Timestamp: base},
				{ID: second, Timestamp: base.Add(time.Minute)},
			}, active)

			missing, err := n.store.ListAggregatesByStatus("missing")
			if !assert.NoError(t, err) {
				return
			}
			assert.Empty(t, missing)

			prod, err := n.store.ListAggregatesByLabel("env", "prod")
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []timebox.AggregateID{first, second}, prod)

			vals, err := n.store.ListLabelValues("env")
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []string{"prod", "stage"}, vals)
		})
	}
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

func TestRestartIndexes(t *testing.T) {
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

	first := timebox.NewAggregateID("order", "1")
	second := timebox.NewAggregateID("order", "2")
	base := time.Unix(1_700_000_000, 0).UTC()

	assert.NoError(t, n.store.AppendEvents(first, 0, []*timebox.Event{
		indexedEvent(first, "active", "prod", base),
	}))
	assert.NoError(t, n.store.AppendEvents(second, 0, []*timebox.Event{
		indexedEvent(second, "paused", "stage", base.Add(time.Minute)),
	}))

	closeNode(t, n)

	n = newNode(t, cfg)
	if n == nil {
		return
	}
	if !waitForWrite(t, n.store) {
		return
	}

	status, err := n.store.GetAggregateStatus(first)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "active", status)

	paused, err := n.store.ListAggregatesByStatus("paused")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.StatusEntry{{
		ID:        second,
		Timestamp: base.Add(time.Minute),
	}}, paused)

	stage, err := n.store.ListAggregatesByLabel("env", "stage")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{second}, stage)

	vals, err := n.store.ListLabelValues("env")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"prod", "stage"}, vals)
}

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

	matches, err := filepath.Glob(filepath.Join(dataDir, "raft-wal", "*.wal"))
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

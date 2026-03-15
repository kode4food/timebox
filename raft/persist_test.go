package raft_test

import (
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
		id          string
		addr        string
		forwardAddr string
		dataDir     string
		compactStep int
		trimEvents  bool
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
	err := n.store.AppendEvents(id, 0, []*timebox.Event{numberEvent(id, 1)})
	if !assert.NoError(t, err) {
		return
	}
	err = n.store.PutSnapshot(id, map[string]int{"value": 1}, 1)
	if !assert.NoError(t, err) {
		return
	}
	err = n.store.AppendEvents(id, 1, []*timebox.Event{numberEvent(id, 2)})
	if !assert.NoError(t, err) {
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

	evs, err := n.store.GetEvents(id, 0)
	if !assert.NoError(t, err) {
		return
	}
	assert.Len(t, evs, 1)
	assert.Equal(t, int64(1), evs[0].Sequence)
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

func TestFollowerStatusReflectsForwardedWrites(t *testing.T) {
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

func TestReadySingleNode(t *testing.T) {
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

func TestDelayedJoinReplicates(t *testing.T) {
	srvs := make([]raft.Server, 0, 3)
	cfgs := make([]nodeConfig, 0, 3)
	for i := range 3 {
		id := fmt.Sprintf("node-%d", i+1)
		addr := freeAddr(t)
		if !assert.NotEmpty(t, addr) {
			return
		}
		forwardAddr := freeAddr(t)
		if !assert.NotEmpty(t, forwardAddr) {
			return
		}
		srvs = append(srvs, raft.Server{
			ID:             id,
			Address:        addr,
			ForwardAddress: forwardAddr,
		})
		cfgs = append(cfgs, nodeConfig{
			id:          id,
			addr:        addr,
			forwardAddr: forwardAddr,
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

func TestRestartAfterCompaction(t *testing.T) {
	const count = 640

	addr := freeAddr(t)
	if addr == "" {
		return
	}

	dataDir := t.TempDir()
	cfg := nodeConfig{
		id:          "node-1",
		addr:        addr,
		dataDir:     dataDir,
		compactStep: 512,
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
		return err == nil &&
			count > 0 &&
			count <= raft.DefaultSnapshotRetain
	}, 15*time.Second, 100*time.Millisecond)

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
	forwardAddr := cfg.forwardAddr
	if forwardAddr == "" {
		forwardAddr = freeAddr(t)
		if forwardAddr == "" {
			return nil
		}
	}

	tbCfg := raft.Config{
		LocalID:                 cfg.id,
		DataDir:                 dataDir,
		BindAddress:             addr,
		ForwardBindAddress:      forwardAddr,
		ForwardAdvertiseAddress: forwardAddr,
		Bootstrap:               true,
		ApplyTimeout:            5 * time.Second,
		CompactMinStep:          cfg.compactStep,
		Timebox: timebox.Config{
			Indexer: combinedIndexer,
			Snapshot: timebox.SnapshotConfig{
				TrimEvents: cfg.trimEvents,
			},
		},
	}

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
		forwardAddr := freeAddr(t)
		if !assert.NotEmpty(t, forwardAddr) {
			return nil
		}
		srvs = append(srvs, raft.Server{
			ID:             id,
			Address:        addr,
			ForwardAddress: forwardAddr,
		})
		cfgs = append(cfgs, nodeConfig{
			id:          id,
			addr:        addr,
			forwardAddr: forwardAddr,
		})
	}

	for _, cfg := range cfgs {
		tbCfg := raft.Config{
			LocalID:                 cfg.id,
			DataDir:                 t.TempDir(),
			BindAddress:             cfg.addr,
			AdvertiseAddress:        cfg.addr,
			ForwardBindAddress:      cfg.forwardAddr,
			ForwardAdvertiseAddress: cfg.forwardAddr,
			Bootstrap:               true,
			Servers:                 srvs,
			ApplyTimeout:            5 * time.Second,
			CompactMinStep:          cfg.compactStep,
			Timebox: timebox.Config{
				Indexer: combinedIndexer,
			},
		}

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

	tbCfg := raft.Config{
		LocalID:                 cfg.id,
		DataDir:                 t.TempDir(),
		BindAddress:             cfg.addr,
		AdvertiseAddress:        cfg.addr,
		ForwardBindAddress:      cfg.forwardAddr,
		ForwardAdvertiseAddress: cfg.forwardAddr,
		Bootstrap:               true,
		Servers:                 srvs,
		ApplyTimeout:            5 * time.Second,
		CompactMinStep:          cfg.compactStep,
		Timebox: timebox.Config{
			Indexer: combinedIndexer,
		},
	}

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

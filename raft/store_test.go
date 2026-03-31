package raft_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/compliance"
	"github.com/kode4food/timebox/raft"
)

func TestStore(t *testing.T) {
	compliance.Run(t, compliance.Profile{
		NewStore: func(
			t *testing.T, cfg compliance.StoreConfig,
		) *timebox.Store {
			t.Helper()

			pCfg := testRaftConfig(nodeConfig{
				id:      "node-1",
				addr:    freeAddr(t),
				dataDir: t.TempDir(),
			})
			storeCfg := testRaftStoreConfig(nodeConfig{
				indexer:    cfg.Indexer,
				trimEvents: cfg.TrimEvents,
			})

			p, err := raft.NewPersistence(pCfg)
			if !assert.NoError(t, err) {
				t.FailNow()
			}

			store, err := p.NewStore(storeCfg)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			t.Cleanup(func() {
				_ = store.Close()
			})
			return store
		},
	})
}

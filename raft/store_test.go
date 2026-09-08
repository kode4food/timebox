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
		Archive: true,
		Open: func(
			t *testing.T, cfg timebox.Config,
		) (timebox.Backend, *timebox.Store) {
			t.Helper()

			pCfg := testRaftConfig(nodeConfig{
				id:      "node-1",
				addr:    freeAddr(t),
				dataDir: t.TempDir(),
			})
			pCfg.Timebox = testRaftTimeboxConfig(nodeConfig{
				indexer:    cfg.Indexer,
				trimEvents: cfg.TrimEvents,
			})

			p, err := raft.NewPersistence(pCfg)
			if !assert.NoError(t, err) {
				t.FailNow()
			}

			store, err := timebox.NewStore(p)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			t.Cleanup(func() {
				_ = store.Close()
			})
			return p, store
		},
	})
}

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/compliance"
	"github.com/kode4food/timebox/memory"
)

func TestStore(t *testing.T) {
	compliance.Run(t, compliance.Profile{
		Archive: true,
		NewStore: func(
			t *testing.T, cfg compliance.StoreConfig,
		) *timebox.Store {
			t.Helper()

			tbCfg := timebox.DefaultConfig()
			tbCfg.Indexer = cfg.Indexer
			tbCfg.TrimEvents = cfg.TrimEvents

			store, err := memory.NewStore(tbCfg)
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

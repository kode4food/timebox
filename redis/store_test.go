package redis_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/compliance"
	tbredis "github.com/kode4food/timebox/redis"
)

func TestStore(t *testing.T) {
	server, err := miniredis.Run()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() {
		server.Close()
	})

	compliance.Run(t, compliance.Profile{
		Archive: true,
		Open: func(
			t *testing.T, cfg compliance.StoreConfig,
		) (timebox.Backend, *timebox.Store) {
			t.Helper()

			pCfg := tbredis.DefaultConfig()
			pCfg.Addr = server.Addr()
			pCfg.Prefix = suitePrefix(t)

			storeCfg := timebox.DefaultConfig()
			storeCfg.Indexer = cfg.Indexer
			storeCfg.TrimEvents = cfg.TrimEvents

			p, err := newPersistence(pCfg)
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
			return p, store
		},
	})
}

func suitePrefix(t *testing.T) string {
	t.Helper()

	name := strings.NewReplacer(
		"/", "-", " ", "-", ":", "-", "(", "-", ")", "-",
	).Replace(t.Name())
	return fmt.Sprintf("store-suite-%s-%d", name, time.Now().UnixNano())
}

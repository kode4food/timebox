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
		NewStore: func(
			t *testing.T, cfg compliance.StoreConfig,
		) *timebox.Store {
			t.Helper()

			tbCfg := tbredis.DefaultConfig()
			tbCfg.Addr = server.Addr()
			tbCfg.Prefix = suitePrefix(t)
			tbCfg.Timebox.Indexer = cfg.Indexer
			tbCfg.Timebox.TrimEvents = cfg.TrimEvents

			store, err := tbredis.NewStore(tbCfg)
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

func suitePrefix(t *testing.T) string {
	t.Helper()

	name := strings.NewReplacer(
		"/", "-", " ", "-", ":", "-", "(", "-", ")", "-",
	).Replace(t.Name())
	return fmt.Sprintf("store-suite-%s-%d", name, time.Now().UnixNano())
}

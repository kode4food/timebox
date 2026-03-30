package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/compliance"
	"github.com/kode4food/timebox/postgres"
)

func TestStore(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		compliance.Run(t, compliance.Profile{
			NewStore: func(
				t *testing.T, tc compliance.StoreConfig,
			) *timebox.Store {
				t.Helper()

				tbCfg := cfg
				tbCfg.Prefix = storeSuitePrefix(t)
				tbCfg.Timebox = timebox.DefaultConfig()
				tbCfg.Timebox.Indexer = tc.Indexer
				tbCfg.Timebox.TrimEvents = tc.TrimEvents

				store, err := postgres.NewStore(tbCfg)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				t.Cleanup(func() {
					_ = store.Close()
				})
				return store
			},
		})
	})
}

func storeSuitePrefix(t *testing.T) string {
	t.Helper()

	name := strings.NewReplacer(
		"/", "-", " ", "-", ":", "-", "(", "-", ")", "-",
	).Replace(t.Name())
	return fmt.Sprintf("store-suite-%s-%d", name, time.Now().UnixNano())
}

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
			Open: func(
				t *testing.T, tc timebox.Config,
			) (timebox.Backend, *timebox.Store) {
				t.Helper()

				pCfg := cfg
				pCfg.Prefix = storeSuitePrefix(t)
				b, err := postgres.Open(pCfg)
				if !assert.NoError(t, err) {
					t.FailNow()
				}

				store, err := timebox.NewStore(b, tc)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				t.Cleanup(func() {
					_ = b.Close()
				})
				return b, store
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

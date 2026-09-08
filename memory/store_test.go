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
		Open: func(
			t *testing.T, cfg timebox.Config,
		) (timebox.Backend, *timebox.Store) {
			t.Helper()

			b := memory.Open()
			store, err := timebox.NewStore(b, cfg)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			t.Cleanup(func() {
				_ = b.Close()
			})
			return b, store
		},
	})
}

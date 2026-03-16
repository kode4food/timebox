package raft_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/raft"
)

func TestConfig(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cfg := raft.DefaultConfig().With(raft.Config{
			LocalID: "node-1",
			Address: "127.0.0.1:9701",
			DataDir: filepath.Join(t.TempDir(), "node-1"),
			Servers: []raft.Server{{
				ID:      "node-1",
				Address: "127.0.0.1:9701",
			}},
		})

		assert.NoError(t, cfg.Validate())
		assert.Equal(t, "127.0.0.1:9701", cfg.ServerAddress())
	})

	t.Run("invalid", func(t *testing.T) {
		tests := []struct {
			name string
			cfg  raft.Config
			err  error
		}{
			{
				name: "missing local ID",
				cfg: raft.DefaultConfig().With(raft.Config{
					Address: "127.0.0.1:9701",
					DataDir: t.TempDir(),
				}),
				err: raft.ErrLocalIDRequired,
			},
			{
				name: "missing data dir",
				cfg: raft.DefaultConfig().With(raft.Config{
					LocalID: "node-1",
					Address: "127.0.0.1:9701",
				}),
				err: raft.ErrDataDirRequired,
			},
			{
				name: "missing address",
				cfg: raft.DefaultConfig().With(raft.Config{
					LocalID: "node-1",
					DataDir: t.TempDir(),
				}),
				err: raft.ErrAddressRequired,
			},
			{
				name: "invalid address",
				cfg: raft.DefaultConfig().With(raft.Config{
					LocalID: "node-1",
					Address: "127.0.0.1",
					DataDir: t.TempDir(),
				}),
				err: raft.ErrInvalidAddress,
			},
			{
				name: "missing local bootstrap server",
				cfg: raft.DefaultConfig().With(raft.Config{
					LocalID: "node-1",
					Address: "127.0.0.1:9701",
					DataDir: t.TempDir(),
					Servers: []raft.Server{{
						ID:      "node-2",
						Address: "127.0.0.1:9702",
					}},
				}),
				err: raft.ErrBootstrapMissingLocalServer,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				err := tt.cfg.Validate()
				assert.ErrorIs(t, err, tt.err)
			})
		}
	})
}

func TestNewStore(t *testing.T) {
	cfg := raft.DefaultConfig().With(raft.Config{
		LocalID: "node-1",
		Address: freeAddr(t),
		DataDir: t.TempDir(),
	})

	store, err := raft.NewStore(cfg)
	if !assert.NoError(t, err) {
		return
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if !assert.NoError(t, store.WaitReady(ctx)) {
		return
	}

	first := timebox.NewAggregateID("order", "1")
	second := timebox.NewAggregateID("order", "2")
	third := timebox.NewAggregateID("invoice", "1")

	assert.NoError(t, store.AppendEvents(first, 0, []*timebox.Event{
		numberEvent(first, 1),
	}))
	assert.NoError(t, store.AppendEvents(second, 0, []*timebox.Event{
		numberEvent(second, 1),
	}))
	assert.NoError(t, store.AppendEvents(third, 0, []*timebox.Event{
		numberEvent(third, 1),
	}))

	ids, err := store.ListAggregates(timebox.NewAggregateID("order"))
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []timebox.AggregateID{first, second}, ids)
}

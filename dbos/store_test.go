package dbos_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/dbos"
	"github.com/kode4food/timebox/internal/compliance"
)

const testURL = "postgres://localhost:5432/postgres?sslmode=disable"

func TestStore(t *testing.T) {
	cfg := testConfig(t)
	index := 0
	compliance.Run(t, compliance.Profile{
		Open: func(
			t *testing.T, tc timebox.Config, pub timebox.Publisher,
		) (timebox.Backend, *timebox.Store) {
			t.Helper()
			index++
			cfg := cfg
			cfg.Namespace = fmt.Sprintf("timebox_%d", index)
			cfg.Publisher = pub
			b := openBackend(t, cfg)
			s, err := b.NewStore(tc)
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			return b, s
		},
	})
}

func TestConfig(t *testing.T) {
	t.Run("Namespace", func(t *testing.T) {
		_, err := dbos.Open(dbos.Config{Namespace: "bad-name"})
		assert.ErrorIs(t, err, dbos.ErrInvalidNamespace)
	})
	t.Run("Pool", func(t *testing.T) {
		_, err := dbos.Open(dbos.Config{MaxConns: 1})
		assert.ErrorIs(t, err, dbos.ErrInvalidMaxConns)
	})
	t.Run("URL", func(t *testing.T) {
		_, err := dbos.Open(dbos.Config{URL: "!"})
		assert.Error(t, err)
	})
}

func TestReplay(t *testing.T) {
	cfg := testConfig(t)
	cfg.Namespace = "replay"
	first := openBackend(t, cfg)
	second := openBackend(t, cfg)
	var wg sync.WaitGroup
	for i := range 36 {
		wg.Go(func() {
			backend := first
			if i%2 == 0 {
				backend = second
			}
			req := timebox.AppendRequest{
				ID:     timebox.NewAggregateID("replay", timebox.ID(strconv.Itoa(i))),
				Events: []*timebox.Event{{Type: "recorded"}},
			}
			assert.NoError(t, backend.Append(req))
		})
	}
	wg.Wait()
	assert.NoError(t, first.Close())
	assert.NoError(t, second.Close())

	reopened := openBackend(t, cfg)
	assert.NoError(t, reopened.Append())
	for i := range 36 {
		id := timebox.NewAggregateID("replay", timebox.ID(strconv.Itoa(i)))
		res, err := reopened.LoadEvents(timebox.LoadEventsRequest{ID: id})
		if assert.NoError(t, err) {
			assert.Len(t, res.Events, 1)
		}
	}
}

func testConfig(t *testing.T) dbos.Config {
	t.Helper()
	ctx := t.Context()
	admin, err := pgxpool.New(ctx, testURL)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(admin.Close)
	name := fmt.Sprintf("timebox_dbos_test_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{name}.Sanitize()
	_, err = admin.Exec(ctx, "CREATE DATABASE "+identifier)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() {
		_, err := admin.Exec(context.Background(), "DROP DATABASE "+identifier+" WITH (FORCE)")
		assert.NoError(t, err)
	})
	u, err := url.Parse(testURL)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	u.Path = "/" + name
	return dbos.Config{
		URL:    u.String(),
		Logger: slog.New(slog.DiscardHandler),
	}
}

func openBackend(t *testing.T, cfg dbos.Config) *dbos.Backend {
	t.Helper()
	b, err := dbos.Open(cfg)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() { assert.NoError(t, b.Close()) })
	return b
}

func newStore(t *testing.T, b *dbos.Backend) *timebox.Store {
	t.Helper()
	s, err := b.NewStore()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return s
}

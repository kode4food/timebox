package redis_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	tbredis "github.com/kode4food/timebox/redis"
)

func TestNewPersistenceBadConfig(t *testing.T) {
	_, err := tbredis.NewPersistence(tbredis.Config{DB: -1})
	assert.ErrorIs(t, err, tbredis.ErrInvalidDB)
}

func TestConsumeArchiveServerDown(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	addr := server.Addr()

	p, err := tbredis.NewPersistence(tbredis.Config{Addr: addr})
	assert.NoError(t, err)
	defer func() { _ = p.Close() }()

	server.Close()
	err = p.ConsumeArchive(context.Background(), func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.Error(t, err)
}

func TestNewPersistencePingError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	addr := server.Addr()
	server.Close()

	p, err := newPersistence(tbredis.Config{Addr: addr})
	assert.Error(t, err)
	assert.Nil(t, p)
}

func TestPersistenceCorruptEvents(t *testing.T) {
	withPersistence(t, func(cfg *tbredis.Config) {
		cfg.Prefix = "corrupt"
	}, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		store, err := p.NewStore(timebox.Config{})
		assert.NoError(t, err)
		id := timebox.NewAggregateID("order", "1")
		err = client.RPush(ctx,
			"corrupt:"+joinAggregateID(id)+":events", "not-json",
		).Err()
		assert.NoError(t, err)

		_, err = p.LoadEvents(timebox.LoadEventsRequest{
			Store:   store,
			ID:      id,
			FromSeq: 0,
		})
		assert.Error(t, err)
	})
}

func TestCorruptSnapshot(t *testing.T) {
	withPersistence(t, func(cfg *tbredis.Config) {
		cfg.Prefix = "corrupt-snapshot"
	}, func(
		ctx context.Context, p *tbredis.Persistence, client *redis.Client,
	) {
		store, err := p.NewStore(timebox.Config{})
		assert.NoError(t, err)
		id := timebox.NewAggregateID("order", "1")
		err = client.Set(ctx,
			"corrupt-snapshot:"+joinAggregateID(id)+":snapshot:val",
			"not-json",
			0,
		).Err()
		assert.NoError(t, err)

		snap, err := p.LoadSnapshot(timebox.LoadSnapshotRequest{
			Store: store,
			ID:    id,
		})
		assert.NoError(t, err)
		assert.Equal(t, json.RawMessage("not-json"), snap.Data)
	})
}

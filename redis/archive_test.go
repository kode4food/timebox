package redis_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	tbredis "github.com/kode4food/timebox/redis"
)

func TestConsumeArchiveMalformed(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		tbredis.Config{Addr: server.Addr()}, timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := tbredis.DefaultPrefix + ":archive"
	_, err = client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{"bad": "data"},
	}).Result()
	assert.NoError(t, err)

	err = store.ConsumeArchive(context.Background(), func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.ErrorIs(t, err, timebox.ErrArchiveRecordMalformed)
}

func TestArchivePayloadBytes(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		tbredis.Config{Addr: server.Addr()}, timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	payload := []byte(`{"id":"order:bytes","snap":"","seq":0,"events":[]}`)
	streamKey := tbredis.DefaultPrefix + ":archive"
	_, err = client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{"payload": payload},
	}).Result()
	assert.NoError(t, err)

	var handled *timebox.ArchiveRecord
	err = store.ConsumeArchive(context.Background(), func(
		_ context.Context, record *timebox.ArchiveRecord,
	) error {
		handled = record
		return nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, handled)
	assert.Equal(t,
		timebox.NewAggregateID("order", "bytes"),
		handled.AggregateID,
	)
}

func TestArchivePayloadJSON(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		tbredis.Config{Addr: server.Addr()}, timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := tbredis.DefaultPrefix + ":archive"
	_, err = client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{"payload": "{bad json"},
	}).Result()
	assert.NoError(t, err)

	err = store.ConsumeArchive(context.Background(), func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.Error(t, err)
}

func TestArchivePayloadBadEvent(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		tbredis.Config{Addr: server.Addr()}, timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	payload := []byte(`{"id":"order:bad","snap":"","seq":0,"events":["{"]}`)
	streamKey := tbredis.DefaultPrefix + ":archive"
	_, err = client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{"payload": payload},
	}).Result()
	assert.NoError(t, err)

	err = store.ConsumeArchive(context.Background(), func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.Error(t, err)
}

func TestArchiveHandlerError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		tbredis.Config{Addr: server.Addr()}, timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "handler-error")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t,
		store.AppendEvents(id, 0, []*timebox.Event{ev}),
	)
	assert.NoError(t, store.Archive(id))

	handlerErr := errors.New("handler failed")
	err = store.ConsumeArchive(ctx, func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return handlerErr
	})
	assert.ErrorIs(t, err, handlerErr)

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := tbredis.DefaultPrefix + ":archive"
	entries, err := client.XRange(ctx, streamKey, "-", "+").Result()
	assert.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestArchivePendingRecovery(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer func() { server.Close() }()

	store, err := newStore(
		tbredis.Config{Addr: server.Addr()}, timebox.Config{},
	)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "pending")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t,
		store.AppendEvents(id, 0, []*timebox.Event{ev}),
	)
	assert.NoError(t, store.Archive(id))

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := tbredis.DefaultPrefix + ":archive"
	group := tbredis.DefaultPrefix + ":archive:group"
	assert.NoError(t,
		client.XGroupCreateMkStream(ctx, streamKey, group, "0-0").Err(),
	)

	now := time.Now().UTC()
	server.SetTime(now)

	reads, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "other-consumer",
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    10 * time.Millisecond,
	}).Result()
	assert.NoError(t, err)
	assert.Len(t, reads, 1)
	assert.Len(t, reads[0].Messages, 1)

	server.SetTime(now.Add(tbredis.DefaultMinIdle + time.Second))

	var handled *timebox.ArchiveRecord
	err = store.ConsumeArchive(ctx, func(
		_ context.Context, record *timebox.ArchiveRecord,
	) error {
		handled = record
		return nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, handled)
	assert.Equal(t, id, handled.AggregateID)

	entries, err := client.XRange(ctx, streamKey, "-", "+").Result()
	assert.NoError(t, err)
	assert.Len(t, entries, 0)
}

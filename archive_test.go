package timebox_test

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
)

func TestArchiveDisabled(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = false

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.Archive(
		context.Background(), timebox.NewAggregateID("test", "disabled"),
	)
	assert.ErrorIs(t, err, timebox.ErrArchivingDisabled)
}

func TestArchiveToStream(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.TrimEvents = true
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "1")

	err = store.PutSnapshot(ctx, id, map[string]int{"value": 3}, 2)
	assert.NoError(t, err)

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t,
		store.AppendEvents(ctx, id, 2, []*timebox.Event{ev}),
	)

	err = store.Archive(ctx, id)
	assert.NoError(t, err)

	events, err := store.GetEvents(ctx, id, 0)
	assert.NoError(t, err)
	assert.Len(t, events, 0)

	aggregates, err := store.ListAggregates(ctx, id)
	assert.NoError(t, err)
	assert.Empty(t, aggregates)

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := storeCfg.Prefix + ":archive"
	entries, err := client.XRange(ctx, streamKey, "-", "+").Result()
	assert.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestConsumeArchive(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("order", "consume")

	ev := &timebox.Event{
		Timestamp: time.Now(),
		Type:      "event.test",
		Data:      json.RawMessage(`{"value":1}`),
	}
	assert.NoError(t,
		store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}),
	)
	assert.NoError(t, store.Archive(ctx, id))

	var handled *timebox.ArchiveRecord
	err = store.ConsumeArchive(
		ctx,
		func(_ context.Context, record *timebox.ArchiveRecord) error {
			handled = record
			return nil
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, handled)
	assert.Equal(t, id, handled.AggregateID)
	assert.Len(t, handled.Events, 1)

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := storeCfg.Prefix + ":archive"
	entries, err := client.XRange(ctx, streamKey, "-", "+").Result()
	assert.NoError(t, err)
	assert.Len(t, entries, 0)
}

func TestConsumeArchiveNoHandler(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.ConsumeArchive(context.Background(), nil)
	assert.Error(t, err)
}

func TestConsumeArchiveMalformed(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := storeCfg.Prefix + ":archive"
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

func TestConsumeArchivePayloadBytes(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	payload := []byte(`{"id":"order:bytes","snap":"","seq":0,"events":["{}"]}`)
	streamKey := storeCfg.Prefix + ":archive"
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
		timebox.NewAggregateID("order", "bytes"), handled.AggregateID,
	)
}

func TestConsumeArchiveInvalidPayloadJSON(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := storeCfg.Prefix + ":archive"
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

func TestConsumeArchiveNoMessages(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	called := false
	err = store.PollArchive(context.Background(), 5*time.Millisecond, func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		called = true
		return nil
	})
	assert.NoError(t, err)
	assert.False(t, called)
}

func TestConsumeArchiveHandlerError(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
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
		store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}),
	)
	assert.NoError(t, store.Archive(ctx, id))

	handlerErr := errors.New("handler failed")
	err = store.ConsumeArchive(ctx, func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return handlerErr
	})
	assert.ErrorIs(t, err, handlerErr)

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := storeCfg.Prefix + ":archive"
	entries, err := client.XRange(ctx, streamKey, "-", "+").Result()
	assert.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestConsumeArchiveDisabled(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = false

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	assert.NoError(t, err)
	defer func() { _ = store.Close() }()

	err = store.ConsumeArchive(context.Background(), func(
		_ context.Context, _ *timebox.ArchiveRecord,
	) error {
		return nil
	})
	assert.ErrorIs(t, err, timebox.ErrArchivingDisabled)
}

func TestPollArchivePendingRecovery(t *testing.T) {
	server, err := miniredis.Run()
	assert.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()
	storeCfg.Archiving = true

	tb, err := timebox.NewTimebox(cfg)
	assert.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
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
		store.AppendEvents(ctx, id, 0, []*timebox.Event{ev}),
	)
	assert.NoError(t, store.Archive(ctx, id))

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	streamKey := storeCfg.Prefix + ":archive"
	group := storeCfg.Prefix + ":archive:group"
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

	server.SetTime(now.Add(timebox.DefaultMinIdle + time.Second))

	var handled *timebox.ArchiveRecord
	err = store.PollArchive(ctx, 50*time.Millisecond, func(
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

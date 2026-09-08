package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kode4food/timebox"
)

type archivePayload struct {
	AggregateID      string   `json:"id"`
	SnapshotData     string   `json:"snap"`
	Events           []string `json:"events"`
	SnapshotSequence int64    `json:"seq"`
}

// DefaultMinIdle is the idle duration before pending archive work is reclaimed
const DefaultMinIdle = 30 * time.Second

func (b *Backend) Archive(id timebox.AggregateID) error {
	snapKey := b.buildKey(id, snapshotValSuffix)
	snapSeqKey := b.buildKey(id, snapshotSeqSuffix)
	eventsKey := b.buildKey(id, eventsSuffix)
	statusKey := b.buildStatusHashKey()
	tagStateKey := b.buildTagStateKey(id)
	tagRootKey := b.buildTagRootKey()
	streamKey := b.archiveStreamKey()

	keys := []string{
		snapKey, snapSeqKey, eventsKey, streamKey, statusKey, tagStateKey,
		tagRootKey,
	}
	args := []any{joinAggregateID(id)}

	result, err := b.publishArchive.Run(
		context.Background(), b.client, keys, args...,
	).Result()
	if err != nil {
		return err
	}

	res := result.([]any)
	if len(res) == 0 {
		return errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}
	return nil
}

func (b *Backend) ConsumeArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) error {
	if handler == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	streamKey := b.archiveStreamKey()
	group := b.archiveGroup()
	if err := b.ensureArchiveGroup(ctx, streamKey, group); err != nil {
		return err
	}

	rec, err := b.resumeArchive(ctx, handler)
	if err != nil || rec {
		return err
	}

	rec, err = b.recoverArchive(ctx, handler)
	if err != nil || rec {
		return err
	}

	return b.pollNewArchive(ctx, handler)
}

func (b *Backend) pollNewArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) error {
	streamKey := b.archiveStreamKey()
	group := b.archiveGroup()
	consumer := b.archiveConsumer()
	for {
		block := archiveReadBlock(ctx)
		args := &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    block,
		}

		streams, err := b.client.XReadGroup(ctx, args).Result()
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			return err
		}

		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}

		return b.handleArchive(ctx,
			streamKey, group, streams[0].Messages[0], handler,
		)
	}
}

func (b *Backend) resumeArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) (bool, error) {
	stream := b.archiveStreamKey()
	group := b.archiveGroup()
	consumer := b.archiveConsumer()
	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, "0"},
		Count:    1,
	}

	streams, err := b.client.XReadGroup(ctx, args).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return false, nil
	}

	return true, b.handleArchive(ctx,
		stream, group, streams[0].Messages[0], handler,
	)
}

func (b *Backend) recoverArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) (bool, error) {
	stream := b.archiveStreamKey()
	group := b.archiveGroup()
	consumer := b.archiveConsumer()
	args := &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  DefaultMinIdle,
		Start:    "0-0",
		Count:    1,
	}

	msgs, _, err := b.client.XAutoClaim(ctx, args).Result()
	if err != nil || len(msgs) == 0 {
		return false, err
	}

	return true, b.handleArchive(ctx, stream, group, msgs[0], handler)
}

func (b *Backend) handleArchive(
	ctx context.Context, stream, group string, msg redis.XMessage,
	handler timebox.ArchiveHandler,
) error {
	record, err := b.parseArchiveRecord(msg)
	if err != nil {
		return err
	}

	if err := handler(ctx, record); err != nil {
		return err
	}

	_, err = b.consumeArchive.Run(
		ctx, b.client, []string{stream}, group, msg.ID,
	).Result()
	return err
}

func (b *Backend) parseArchiveRecord(
	msg redis.XMessage,
) (*timebox.ArchiveRecord, error) {
	payloadRaw, ok := msg.Values["payload"]
	if !ok {
		return nil, timebox.ErrArchiveRecordMalformed
	}

	payloadBytes, ok := payloadRaw.(string)
	if !ok {
		rawBytes, ok := payloadRaw.([]byte)
		if !ok {
			return nil, timebox.ErrArchiveRecordMalformed
		}
		payloadBytes = string(rawBytes)
	}

	var payload archivePayload
	if err := json.Unmarshal([]byte(payloadBytes), &payload); err != nil {
		return nil, err
	}

	record := &timebox.ArchiveRecord{
		StreamID:         msg.ID,
		AggregateID:      parseAggregateID(payload.AggregateID),
		SnapshotData:     json.RawMessage(payload.SnapshotData),
		SnapshotSequence: payload.SnapshotSequence,
		Events:           make([]*timebox.Event, 0, len(payload.Events)),
	}

	for _, item := range payload.Events {
		ev, err := timebox.JSONEvent.Decode([]byte(item))
		if err != nil {
			return nil, err
		}
		record.Events = append(record.Events, ev)
	}

	return record, nil
}

func (b *Backend) ensureArchiveGroup(
	ctx context.Context, streamKey, group string,
) error {
	err := b.client.XGroupCreateMkStream(ctx, streamKey, group, "0-0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func archiveReadBlock(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}

	block := time.Until(deadline)
	if block <= 0 {
		return time.Millisecond
	}
	return block
}

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
	SnapshotSequence int64    `json:"seq"`
	Events           []string `json:"events"`
}

// DefaultMinIdle is the idle duration before pending archive work is reclaimed
const DefaultMinIdle = 30 * time.Second

func (p *Persistence) Archive(id timebox.AggregateID) error {
	snapKey := p.buildKey(id, snapshotValSuffix)
	snapSeqKey := p.buildKey(id, snapshotSeqSuffix)
	eventsKey := p.buildKey(id, eventsSuffix)
	statusKey := p.buildStatusHashKey()
	labelStateKey := p.buildLabelStateKey(id)
	labelRootKey := p.buildLabelRootKey()
	streamKey := p.archiveStreamKey()

	keys := []string{
		snapKey, snapSeqKey, eventsKey, streamKey, statusKey, labelStateKey,
		labelRootKey,
	}
	args := []any{joinAggregateID(id)}

	result, err := p.publishArchive.Run(
		context.Background(), p.client, keys, args...,
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

func (p *Persistence) ConsumeArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) error {
	if handler == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	streamKey := p.archiveStreamKey()
	group := p.archiveGroup()
	if err := p.ensureArchiveGroup(ctx, streamKey, group); err != nil {
		return err
	}

	rec, err := p.resumeArchive(ctx, handler)
	if err != nil || rec {
		return err
	}

	rec, err = p.recoverArchive(ctx, handler)
	if err != nil || rec {
		return err
	}

	return p.pollNewArchive(ctx, handler)
}

func (p *Persistence) pollNewArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) error {
	streamKey := p.archiveStreamKey()
	group := p.archiveGroup()
	consumer := p.archiveConsumer()
	for {
		block := archiveReadBlock(ctx)
		args := &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{streamKey, ">"},
			Count:    1,
			Block:    block,
		}

		streams, err := p.client.XReadGroup(ctx, args).Result()
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

		return p.handleArchive(ctx,
			streamKey, group, streams[0].Messages[0], handler,
		)
	}
}

func (p *Persistence) resumeArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) (bool, error) {
	stream := p.archiveStreamKey()
	group := p.archiveGroup()
	consumer := p.archiveConsumer()
	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, "0"},
		Count:    1,
	}

	streams, err := p.client.XReadGroup(ctx, args).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return false, nil
	}

	return true, p.handleArchive(ctx,
		stream, group, streams[0].Messages[0], handler,
	)
}

func (p *Persistence) recoverArchive(
	ctx context.Context, handler timebox.ArchiveHandler,
) (bool, error) {
	stream := p.archiveStreamKey()
	group := p.archiveGroup()
	consumer := p.archiveConsumer()
	args := &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  DefaultMinIdle,
		Start:    "0-0",
		Count:    1,
	}

	msgs, _, err := p.client.XAutoClaim(ctx, args).Result()
	if err != nil || len(msgs) == 0 {
		return false, err
	}

	return true, p.handleArchive(ctx, stream, group, msgs[0], handler)
}

func (p *Persistence) handleArchive(
	ctx context.Context, stream, group string, msg redis.XMessage,
	handler timebox.ArchiveHandler,
) error {
	record, err := p.parseArchiveRecord(msg)
	if err != nil {
		return err
	}

	if err := handler(ctx, record); err != nil {
		return err
	}

	_, err = p.consumeArchive.Run(
		ctx, p.client, []string{stream}, group, msg.ID,
	).Result()
	return err
}

func (p *Persistence) parseArchiveRecord(
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

func (p *Persistence) ensureArchiveGroup(
	ctx context.Context, streamKey, group string,
) error {
	err := p.client.XGroupCreateMkStream(ctx, streamKey, group, "0-0").Err()
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

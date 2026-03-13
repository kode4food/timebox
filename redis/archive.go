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
	if !p.config.Timebox.Archiving {
		return timebox.ErrArchivingDisabled
	}

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
	args := []any{p.config.JoinKey(id)}

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
	return p.PollArchive(ctx, 0, handler)
}

func (p *Persistence) PollArchive(
	ctx context.Context, timeout time.Duration, handler timebox.ArchiveHandler,
) error {
	if !p.config.Timebox.Archiving {
		return timebox.ErrArchivingDisabled
	}
	if handler == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	streamKey := p.archiveStreamKey()
	group := p.archiveGroup()
	consumer := p.archiveConsumer()

	if err := p.ensureArchiveGroup(ctx, streamKey, group); err != nil {
		return err
	}

	rec, err := p.recoverArchive(ctx, streamKey, group, consumer, handler)
	if err != nil || rec {
		return err
	}

	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    timeout,
	}

	streams, err := p.client.XReadGroup(ctx, args).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil
	}

	return p.handleArchive(ctx,
		streamKey, group, streams[0].Messages[0], handler,
	)
}

func (p *Persistence) recoverArchive(
	ctx context.Context, stream, group, consumer string,
	handler timebox.ArchiveHandler,
) (bool, error) {
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
		if rawBytes, okBytes := payloadRaw.([]byte); okBytes {
			payloadBytes = string(rawBytes)
		} else {
			return nil, timebox.ErrArchiveRecordMalformed
		}
	}

	var payload archivePayload
	if err := json.Unmarshal([]byte(payloadBytes), &payload); err != nil {
		return nil, err
	}

	record := &timebox.ArchiveRecord{
		StreamID:         msg.ID,
		AggregateID:      p.config.ParseKey(payload.AggregateID),
		SnapshotData:     json.RawMessage(payload.SnapshotData),
		SnapshotSequence: payload.SnapshotSequence,
		Events:           make([]json.RawMessage, 0, len(payload.Events)),
	}

	for _, ev := range payload.Events {
		record.Events = append(record.Events, json.RawMessage(ev))
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

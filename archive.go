package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type (
	// ArchiveRecord stores stream metadata and aggregate artifacts
	ArchiveRecord struct {
		StreamID         string
		AggregateID      AggregateID
		SnapshotData     json.RawMessage
		SnapshotSequence int64
		Events           []json.RawMessage
	}

	archivePayload struct {
		AggregateID      string   `json:"id"`
		SnapshotData     string   `json:"snap"`
		SnapshotSequence int64    `json:"seq"`
		Events           []string `json:"events"`
	}

	// ArchiveHandler handles a single archive record
	ArchiveHandler func(context.Context, *ArchiveRecord) error
)

var (
	// ErrArchivingDisabled indicates Archive is not enabled for the Store
	ErrArchivingDisabled = errors.New("archiving not enabled for this store")

	// ErrArchiveRecordMalformed indicates an archive record was malformed
	ErrArchiveRecordMalformed = errors.New("archive record malformed")
)

// DefaultMinIdle is the idle duration before pending archive work is reclaimed
const DefaultMinIdle = 30 * time.Second

// Archive moves aggregate artifacts to a Redis stream and clears the
// aggregate's snapshot/events on success
func (s *Store) Archive(ctx context.Context, id AggregateID) error {
	if !s.config.Archiving {
		return ErrArchivingDisabled
	}

	snapKey := s.buildKey(id, snapshotValSuffix)
	snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
	eventsKey := s.buildKey(id, eventsSuffix)
	streamKey := s.archiveStreamKey()

	keys := []string{snapKey, snapSeqKey, eventsKey, streamKey}
	args := []any{id.Join(":")}

	result, err := s.publishArchive.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return err
	}

	res := result.([]any)
	if len(res) == 0 {
		return ErrUnexpectedLuaResult
	}
	return nil
}

// ConsumeArchive reads one archive record from the stream and invokes handler.
// If handler succeeds, the stream entry is acknowledged and deleted
func (s *Store) ConsumeArchive(
	ctx context.Context, handler ArchiveHandler,
) error {
	return s.PollArchive(ctx, 0, handler)
}

// PollArchive reads one archive record using the provided timeout. If handler
// succeeds, the stream entry is acknowledged and deleted
func (s *Store) PollArchive(
	ctx context.Context, timeout time.Duration, handler ArchiveHandler,
) error {
	if !s.config.Archiving {
		return ErrArchivingDisabled
	}
	if handler == nil {
		return errors.New("archive handler is required")
	}

	streamKey := s.archiveStreamKey()
	group := s.archiveGroup()
	consumer := s.archiveConsumer()

	if err := s.ensureArchiveGroup(ctx, streamKey, group); err != nil {
		return err
	}

	rec, err := s.recoverArchive(ctx, streamKey, group, consumer, handler)
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

	streams, err := s.client.XReadGroup(ctx, args).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil
	}

	return s.handleArchive(
		ctx, streamKey, group, streams[0].Messages[0], handler,
	)
}

func (s *Store) recoverArchive(
	ctx context.Context, stream, group, consumer string, handler ArchiveHandler,
) (bool, error) {
	args := &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  DefaultMinIdle,
		Start:    "0-0",
		Count:    1,
	}

	msgs, _, err := s.client.XAutoClaim(ctx, args).Result()
	if err != nil || len(msgs) == 0 {
		return false, err
	}

	return true, s.handleArchive(ctx, stream, group, msgs[0], handler)
}

func (s *Store) handleArchive(
	ctx context.Context, stream, group string, msg redis.XMessage,
	handler ArchiveHandler,
) error {
	record, err := s.parseArchiveRecord(msg)
	if err != nil {
		return err
	}

	if err := handler(ctx, record); err != nil {
		return err
	}

	_, err = s.consumeArchive.Run(
		ctx, s.client, []string{stream}, group, msg.ID,
	).Result()
	return err
}

func (s *Store) parseArchiveRecord(msg redis.XMessage) (*ArchiveRecord, error) {
	payloadRaw, ok := msg.Values["payload"]
	if !ok {
		return nil, ErrArchiveRecordMalformed
	}

	payloadBytes, ok := payloadRaw.(string)
	if !ok {
		if rawBytes, okBytes := payloadRaw.([]byte); okBytes {
			payloadBytes = string(rawBytes)
		} else {
			return nil, ErrArchiveRecordMalformed
		}
	}

	var payload archivePayload
	if err := json.Unmarshal([]byte(payloadBytes), &payload); err != nil {
		return nil, err
	}

	record := &ArchiveRecord{
		StreamID:         msg.ID,
		AggregateID:      s.parseAggregateID(payload.AggregateID),
		SnapshotData:     json.RawMessage(payload.SnapshotData),
		SnapshotSequence: payload.SnapshotSequence,
		Events:           make([]json.RawMessage, 0, len(payload.Events)),
	}

	for _, ev := range payload.Events {
		record.Events = append(record.Events, json.RawMessage(ev))
	}

	return record, nil
}

func (s *Store) ensureArchiveGroup(
	ctx context.Context, streamKey, group string,
) error {
	err := s.client.XGroupCreateMkStream(ctx, streamKey, group, "0-0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

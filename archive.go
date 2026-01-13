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

	// ArchiveHandler handles a single archive record.
	ArchiveHandler func(context.Context, *ArchiveRecord) error
)

var (
	// ErrArchivingDisabled indicates Archive is not enabled for the Store
	ErrArchivingDisabled = errors.New("archiving not enabled for this store")

	// ErrArchiveRecordMalformed indicates an archive record was malformed
	ErrArchiveRecordMalformed = errors.New("archive record malformed")
)

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

	result, err := s.archive.Run(ctx, s.client, keys, args...).Result()
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

	block := timeout
	if block == 0 {
		block = 1 * time.Millisecond
	}

	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    block,
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

	msg := streams[0].Messages[0]
	record, err := s.parseArchiveRecord(msg)
	if err != nil {
		return err
	}

	if err := handler(ctx, record); err != nil {
		return err
	}

	if err := s.client.XAck(ctx, streamKey, group, msg.ID).Err(); err != nil {
		return err
	}
	return s.client.XDel(ctx, streamKey, msg.ID).Err()
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
	err := s.client.XGroupCreateMkStream(ctx, streamKey, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

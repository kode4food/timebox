package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type (
	// ArchiveRecord holds an archived aggregate's bundled artifacts
	ArchiveRecord struct {
		AggregateID      AggregateID
		SnapshotData     json.RawMessage `json:",omitempty"`
		Events           []*Event
		SnapshotSequence int64
	}

	// ArchiveHandler handles a single archive record
	ArchiveHandler func(context.Context, *ArchiveRecord) error
)

const archiveQueue ID = "_tb.archive_"

const archiveRecorded EventType = "_tb.archive.recorded"

// ArchiveQueueID identifies the aggregate a Backend's native Archive
// implementation appends records to, and Store.ConsumeArchive drains
var ArchiveQueueID = NewAggregateID(queueAggregateType, archiveQueue)

var (
	// ErrArchiveRecordMalformed indicates an archive record was malformed
	ErrArchiveRecordMalformed = errors.New("archive record malformed")

	// ErrArchiveHandlerMissing indicates a consume call is missing a handler
	ErrArchiveHandlerMissing = errors.New("archive handler is required")
)

// Archive bundles an aggregate's snapshot and remaining events into the archive
// queue and removes its live records. A missing aggregate is a no-op
func (s *Store) Archive(id AggregateID) error {
	if err := s.backend.Archive(id); err != nil {
		return err
	}
	select {
	case s.queueWake <- struct{}{}:
	default:
	}
	return nil
}

// ConsumeArchive hands the oldest archived record to handler. The record is
// removed only if handler returns nil
func (s *Store) ConsumeArchive(
	ctx context.Context, handler ArchiveHandler,
) error {
	if handler == nil {
		return ErrArchiveHandlerMissing
	}
	return s.Consume(ctx, archiveQueue,
		func(_ *Transaction, msg *Message) error {
			record, err := msg.GetValue[*ArchiveRecord]()
			if err != nil {
				return fmt.Errorf("%w: %v", ErrArchiveRecordMalformed, err)
			}
			return handler(ctx, record)
		},
	)
}

// NewArchiveQueueEvent builds the raw event a Backend's native Archive
// implementation must atomically append to ArchiveQueueID at seq, alongside
// removing id's own live records
func NewArchiveQueueEvent(
	seq int64, id AggregateID, record *ArchiveRecord,
) (*Event, error) {
	data, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	msgData, err := json.Marshal(&Message{
		Type: archiveRecorded, AggregateID: id, Data: data,
	})
	if err != nil {
		return nil, err
	}
	return &Event{
		Sequence:    seq,
		Type:        queueItemAdded,
		AggregateID: ArchiveQueueID,
		Data:        msgData,
	}, nil
}

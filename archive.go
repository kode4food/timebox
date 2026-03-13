package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type (
	// ArchiveRecord stores stream metadata and aggregate artifacts.
	ArchiveRecord struct {
		StreamID         string
		AggregateID      AggregateID
		SnapshotData     json.RawMessage
		SnapshotSequence int64
		Events           []json.RawMessage
	}

	// ArchiveHandler handles a single archive record.
	ArchiveHandler func(context.Context, *ArchiveRecord) error
)

var (
	// ErrArchivingDisabled indicates archiving is not enabled.
	ErrArchivingDisabled = errors.New("archiving not enabled for this store")

	// ErrArchiveRecordMalformed indicates an archive record was malformed.
	ErrArchiveRecordMalformed = errors.New("archive record malformed")

	// ErrArchiveHandlerMissing indicates a consume call is missing a handler.
	ErrArchiveHandlerMissing = errors.New("archive handler is required")
)

// DefaultMinIdle is the idle duration before pending archive work is reclaimed.
const DefaultMinIdle = 30 * time.Second

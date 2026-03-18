package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

type (
	// Backend is used by Store to provide Persistence and Queries
	Backend interface {
		Persistence
		Queries
	}

	// Persistence provides the low-level primitives Store uses to implement
	// Store semantics
	Persistence interface {
		io.Closer

		// Ready reports when the persistence backend can serve requests
		Ready() <-chan struct{}

		// Append atomically appends events if the expected sequence still
		// matches
		Append(AppendRequest) (*AppendResult, error)

		// LoadEvents loads raw persisted events starting at fromSeq
		LoadEvents(id AggregateID, fromSeq int64) (*EventsResult, error)

		// LoadSnapshot loads the raw snapshot and any trailing raw events
		LoadSnapshot(id AggregateID) (*SnapshotRecord, error)

		// SaveSnapshot stores raw snapshot data at the provided sequence
		SaveSnapshot(id AggregateID, data []byte, sequence int64) error
	}

	// Queries provides aggregate and index query operations
	Queries interface {
		// ListAggregates lists aggregate IDs that match the provided prefix
		ListAggregates(id AggregateID) ([]AggregateID, error)

		// GetAggregateStatus loads the current indexed status for an aggregate
		GetAggregateStatus(id AggregateID) (string, error)

		// ListAggregatesByStatus lists aggregates currently indexed by status
		ListAggregatesByStatus(status string) ([]StatusEntry, error)

		// ListAggregatesByLabel lists aggregates currently indexed by
		// label/value
		ListAggregatesByLabel(label, value string) ([]AggregateID, error)

		// ListLabelValues lists the distinct indexed values for a label
		ListLabelValues(label string) ([]string, error)
	}

	// Archiver provides optional archive lifecycle support for Store
	Archiver interface {
		// Archive moves an aggregate's persisted artifacts into archive storage
		Archive(id AggregateID) error

		// ConsumeArchive blocks until one archive record is available or ctx
		// is done
		ConsumeArchive(ctx context.Context, handler ArchiveHandler) error
	}

	// AppendRequest contains primitive inputs required for an atomic append
	AppendRequest struct {
		ID               AggregateID       `json:"id"`
		ExpectedSequence int64             `json:"expected_seq"`
		Status           *string           `json:"status,omitempty"`
		StatusAt         string            `json:"status_at,omitempty"`
		Labels           map[string]string `json:"labels,omitempty"`
		Events           []string          `json:"events"`
	}

	// AppendResult describes an optimistic concurrency conflict. A nil result
	// means the append succeeded
	AppendResult struct {
		ActualSequence int64             `json:"actual_seq"`
		NewEvents      []json.RawMessage `json:"new_events,omitempty"`
	}

	// EventsResult contains raw persisted events and the sequence to assign to
	// the first event in the slice
	EventsResult struct {
		StartSequence int64
		Events        []json.RawMessage
	}

	// SnapshotRecord contains raw snapshot data and any raw trailing events
	SnapshotRecord struct {
		Data     json.RawMessage
		Sequence int64
		Events   []json.RawMessage
	}

	// Index stores optional projection metadata derived from an event
	Index struct {
		// Status represents the resultant aggregate status. nil means no
		// status change, and "" clears any prior status
		Status *string `json:"status,omitempty"`

		// Labels updates current label values for the aggregate. nil means no
		// label changes, and empty values remove the label
		Labels map[string]string `json:"labels,omitempty"`
	}

	// Indexer derives projection metadata for an event batch
	Indexer func([]*Event) []*Index

	// StatusEntry holds an aggregate ID and the time it entered a status
	StatusEntry struct {
		ID        AggregateID
		Timestamp time.Time
	}

	// ArchiveRecord stores stream metadata and aggregate artifacts
	ArchiveRecord struct {
		StreamID         string
		AggregateID      AggregateID
		SnapshotData     json.RawMessage
		SnapshotSequence int64
		Events           []json.RawMessage
	}

	// ArchiveHandler handles a single archive record
	ArchiveHandler func(context.Context, *ArchiveRecord) error
)

var (
	// ErrUnexpectedResult indicates data returned in an unexpected shape
	ErrUnexpectedResult = errors.New("unexpected result")

	// ErrArchivingDisabled indicates archiving is not enabled
	ErrArchivingDisabled = errors.New("archiving not enabled for this store")

	// ErrArchiveRecordMalformed indicates an archive record was malformed
	ErrArchiveRecordMalformed = errors.New("archive record malformed")

	// ErrArchiveHandlerMissing indicates a consume call is missing a handler
	ErrArchiveHandlerMissing = errors.New("archive handler is required")
)

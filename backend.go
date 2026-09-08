package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

type (
	// Backend provides the low-level primitives Store uses to implement
	// Store semantics, along with its query operations
	Backend interface {
		io.Closer
		Queries

		// Ready reports when the backend can serve requests
		Ready() <-chan struct{}

		// Append atomically appends every distinctly named request if each
		// expected sequence still matches. It returns a VersionConflictError
		// naming the first request whose sequence check fails
		Append(...AppendRequest) error

		// LoadEvents loads raw persisted events starting at fromSeq
		LoadEvents(LoadEventsRequest) (*EventsResult, error)

		// LoadSnapshot loads the raw snapshot and any trailing raw events
		LoadSnapshot(LoadSnapshotRequest) (*SnapshotRecord, error)

		// SaveSnapshot stores raw snapshot data using the supplied semantics
		SaveSnapshot(SnapshotRequest) error
	}

	// Queries provides aggregate and index query operations
	Queries interface {
		// ListAggregates lists aggregate IDs of the provided type, or of
		// every type when it is empty
		ListAggregates(typ ID) ([]AggregateID, error)

		// GetAggregateStatus loads the current indexed status for an aggregate
		GetAggregateStatus(id AggregateID) (string, error)

		// ListAggregatesByStatus lists aggregates currently indexed by status
		ListAggregatesByStatus(status string) ([]StatusEntry, error)

		// ListAggregatesByTag lists aggregates currently indexed by tag
		ListAggregatesByTag(tag string) ([]AggregateID, error)
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
		StatusAt         time.Time
		Status           *string
		Tags             map[string]bool
		ID               AggregateID
		Events           []*Event
		ExpectedSequence int64
		TrimEvents       bool
	}

	// LoadEventsRequest contains primitive inputs required for an event load
	LoadEventsRequest struct {
		ID         AggregateID
		FromSeq    int64
		TrimEvents bool
	}

	// EventsResult contains raw persisted events and the sequence to assign to
	// the first event in the slice
	EventsResult struct {
		Events        []*Event
		StartSequence int64
	}

	// LoadSnapshotRequest contains primitive inputs required for a snapshot
	// load
	LoadSnapshotRequest struct {
		ID         AggregateID
		TrimEvents bool
	}

	// SnapshotRecord contains raw snapshot data and any raw trailing events
	SnapshotRecord struct {
		Data     json.RawMessage
		Events   []*Event
		Sequence int64
	}

	// SnapshotRequest contains primitive inputs required for a snapshot save
	SnapshotRequest struct {
		ID         AggregateID
		Data       []byte
		Sequence   int64
		TrimEvents bool
	}

	// Index stores optional projection metadata derived from an event
	Index struct {
		// Status represents the resultant aggregate status. nil means no
		// status change, and "" clears any prior status
		Status *string `json:"status,omitempty"`

		// Tags updates aggregate tag membership. true adds and false removes
		Tags map[string]bool `json:"tags,omitempty"`
	}

	// Indexer derives projection metadata for an event batch
	Indexer func([]*Event) []*Index

	// StatusEntry holds an aggregate ID and the time it entered a status
	StatusEntry struct {
		Timestamp time.Time
		ID        AggregateID
	}

	// ArchiveRecord stores stream metadata and aggregate artifacts
	ArchiveRecord struct {
		StreamID         string
		AggregateID      AggregateID
		SnapshotData     json.RawMessage
		Events           []*Event
		SnapshotSequence int64
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

	// ErrDuplicateAggregate indicates one append names an aggregate twice
	ErrDuplicateAggregate = errors.New("aggregate appended twice")
)

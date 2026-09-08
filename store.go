package timebox

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"
)

type (
	// Store persists, queries, and snapshots aggregate events
	Store struct {
		Queries
		backend Backend
		config  Config
	}

	// VersionConflictError is returned when AppendEvents encounters a sequence
	// mismatch. NewEvents contains the conflicting events
	VersionConflictError struct {
		NewEvents        []*Event
		ID               AggregateID
		ExpectedSequence int64
		ActualSequence   int64
	}

	// SnapshotResult holds the loaded snapshot, the sequence at which it was
	// taken, and any events that need to be applied after it
	SnapshotResult struct {
		AdditionalEvents []*Event
		NextSequence     int64
		SnapshotSize     int
		EventsSize       int
	}
)

// NewStore creates a Store backed by the supplied Backend
func NewStore(b Backend, cfgs ...Config) (*Store, error) {
	cfg := Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Store{
		Queries: b,
		backend: b,
		config:  cfg,
	}, nil
}

// Config returns the Store configuration
func (s *Store) Config() Config {
	return s.config
}

// Ready reports when the underlying Backend can serve requests
func (s *Store) Ready() <-chan struct{} {
	return s.backend.Ready()
}

// WaitReady blocks until the underlying Backend can serve requests
func (s *Store) WaitReady(ctx context.Context) error {
	select {
	case <-s.Ready():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AppendEvents atomically appends events for an aggregate if the expected
// sequence matches the current log sequence
func (s *Store) AppendEvents(id AggregateID, atSeq int64, evs []*Event) error {
	return s.backend.Append(s.appendRequest(id, atSeq, evs))
}

// GetEvents returns all events for an aggregate starting at fromSeq
func (s *Store) GetEvents(id AggregateID, fromSeq int64) ([]*Event, error) {
	res, err := s.backend.LoadEvents(LoadEventsRequest{
		ID:         id,
		FromSeq:    fromSeq,
		TrimEvents: s.config.TrimEvents,
	})
	if err != nil {
		return nil, err
	}
	if len(res.Events) == 0 {
		return []*Event{}, nil
	}
	return res.Events, nil
}

// GetSnapshot loads the latest snapshot into target and returns any events
// stored after the snapshot sequence
func (s *Store) GetSnapshot(
	id AggregateID, target any,
) (*SnapshotResult, error) {
	rec, err := s.backend.LoadSnapshot(LoadSnapshotRequest{
		ID:         id,
		TrimEvents: s.config.TrimEvents,
	})
	if err != nil {
		return nil, err
	}

	if len(rec.Data) > 0 {
		if err := json.Unmarshal(rec.Data, target); err != nil {
			return nil, err
		}
	}

	eventsSize := 0
	for _, ev := range rec.Events {
		eventsSize += len(ev.Data)
	}

	return &SnapshotResult{
		AdditionalEvents: rec.Events,
		NextSequence:     rec.Sequence,
		SnapshotSize:     len(rec.Data),
		EventsSize:       eventsSize,
	}, nil
}

// PutSnapshot saves a snapshot value and sequence if the provided sequence is
// newer than any stored snapshot
func (s *Store) PutSnapshot(id AggregateID, value any, sequence int64) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.backend.SaveSnapshot(SnapshotRequest{
		ID:         id,
		Data:       data,
		Sequence:   sequence,
		TrimEvents: s.config.TrimEvents,
	})
}

// Archive moves aggregate artifacts to persistent archive storage
func (s *Store) Archive(id AggregateID) error {
	archiver, ok := s.backend.(Archiver)
	if !ok {
		return ErrArchivingDisabled
	}
	return archiver.Archive(id)
}

// ConsumeArchive reads one archive record and invokes handler
func (s *Store) ConsumeArchive(
	ctx context.Context, handler ArchiveHandler,
) error {
	archiver, ok := s.backend.(Archiver)
	if !ok {
		return ErrArchivingDisabled
	}
	return archiver.ConsumeArchive(ctx, handler)
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf(
		"version conflict: expected sequence %d, but at %d (%d new events)",
		e.ExpectedSequence, e.ActualSequence, len(e.NewEvents),
	)
}

// appendRequest sequences the events and derives the index metadata an append
// carries, without performing the append
func (s *Store) appendRequest(
	id AggregateID, atSeq int64, evs []*Event,
) AppendRequest {
	evs = sequenceEvents(id, atSeq, evs)

	var status *string
	var statusAt time.Time
	tags := map[string]bool{}

	if len(evs) > 0 && s.config.Indexer != nil {
		idxs := s.config.Indexer(evs)

		for _, idx := range idxs {
			if idx != nil && idx.Status != nil {
				status = idx.Status
			}
			if idx != nil {
				maps.Copy(tags, idx.Tags)
			}
		}
	}

	if status != nil {
		statusAt = evs[len(evs)-1].Timestamp.UTC()
	}

	return AppendRequest{
		ID:               id,
		ExpectedSequence: atSeq,
		Status:           status,
		StatusAt:         statusAt,
		Tags:             tags,
		Events:           evs,
		TrimEvents:       s.config.TrimEvents,
	}
}

func sequenceEvents(id AggregateID, atSeq int64, evs []*Event) []*Event {
	res := make([]*Event, len(evs))
	for idx, ev := range evs {
		res[idx] = &Event{
			Timestamp:   ev.Timestamp,
			Sequence:    atSeq + int64(idx),
			Type:        ev.Type,
			AggregateID: id,
			Data:        ev.Data,
			Raised:      ev.Raised,
			value:       ev.value,
		}
	}
	return res
}

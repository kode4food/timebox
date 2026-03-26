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
		persistence    Persistence
		snapshotWorker *SnapshotWorker
		config         Config
	}

	// VersionConflictError is returned when AppendEvents encounters a sequence
	// mismatch. NewEvents contains the conflicting events
	VersionConflictError struct {
		NewEvents        []*Event
		ExpectedSequence int64
		ActualSequence   int64
	}

	// SnapshotResult holds the loaded snapshot, the sequence at which it was
	// taken, and any events that need to be applied after it
	SnapshotResult struct {
		AdditionalEvents []*Event
		NextSequence     int64
		ShouldSnapshot   bool
	}

	snapshotSaver interface {
		CanSaveSnapshot() bool
	}
)

// NewStore creates a Store backed by the supplied Backend
func NewStore(b Backend, cfg Config) (*Store, error) {
	cfg = Configure(DefaultConfig(), cfg)
	if err := cfg.validateStore(); err != nil {
		return nil, err
	}
	return newStore(cfg, b), nil
}

func newStore(cfg Config, b Backend) *Store {
	s := &Store{
		Queries:     b,
		persistence: b,
		config:      cfg,
	}
	if cfg.Snapshot.Workers {
		s.snapshotWorker = newSnapshotWorker(s)
	}
	return s
}

// Config returns the Store configuration
func (s *Store) Config() Config {
	return s.config
}

// Ready reports when the underlying persistence can serve requests
func (s *Store) Ready() <-chan struct{} {
	return s.persistence.Ready()
}

// WaitReady blocks until the underlying persistence can serve requests
func (s *Store) WaitReady(ctx context.Context) error {
	select {
	case <-s.Ready():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops background workers and closes the underlying persistence
func (s *Store) Close() error {
	if s.snapshotWorker != nil {
		s.snapshotWorker.Stop()
	}
	return s.persistence.Close()
}

// AppendEvents atomically appends events for an aggregate if the expected
// sequence matches the current log sequence
func (s *Store) AppendEvents(id AggregateID, atSeq int64, evs []*Event) error {
	evs = sequenceEvents(id, atSeq, evs)
	if len(evs) == 0 {
		return nil
	}

	var idxs []*Index
	if s.config.Indexer != nil {
		idxs = s.config.Indexer(evs)
	}

	var status *string
	var statusAt time.Time
	lbls := map[string]string{}
	for _, idx := range idxs {
		if idx != nil && idx.Status != nil {
			status = idx.Status
		}
		if idx != nil {
			maps.Copy(lbls, idx.Labels)
		}
	}
	if status != nil {
		statusAt = evs[len(evs)-1].Timestamp.UTC()
	}

	return s.persistence.Append(AppendRequest{
		ID:               id,
		ExpectedSequence: atSeq,
		Status:           status,
		StatusAt:         statusAt,
		Labels:           lbls,
		Events:           evs,
	})
}

// GetEvents returns all events for an aggregate starting at fromSeq
func (s *Store) GetEvents(id AggregateID, fromSeq int64) ([]*Event, error) {
	res, err := s.persistence.LoadEvents(id, fromSeq)
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
	rec, err := s.persistence.LoadSnapshot(id)
	if err != nil {
		return nil, err
	}

	if len(rec.Data) > 0 {
		if err := json.Unmarshal(rec.Data, target); err != nil {
			return nil, err
		}
	}

	events := rec.Events

	eventsSize := 0
	for _, item := range rec.Events {
		eventsSize += len(item.Data)
	}

	return &SnapshotResult{
		AdditionalEvents: events,
		NextSequence:     rec.Sequence,
		ShouldSnapshot:   eventsSize > len(rec.Data),
	}, nil
}

// PutSnapshot saves a snapshot value and sequence if the provided sequence is
// newer than any stored snapshot
func (s *Store) PutSnapshot(
	id AggregateID, value any, sequence int64,
) error {
	return s.writeSnapshot(id, value, sequence)
}

func (s *Store) writeSnapshot(
	id AggregateID, value any, sequence int64,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.persistence.SaveSnapshot(id, data, sequence)
}

func (s *Store) canSaveSnapshot() bool {
	if p, ok := s.persistence.(snapshotSaver); ok {
		return p.CanSaveSnapshot()
	}
	return true
}

// Archive moves aggregate artifacts to persistent archive storage
func (s *Store) Archive(id AggregateID) error {
	archiver, ok := s.persistence.(Archiver)
	if !ok {
		return ErrArchivingDisabled
	}
	return archiver.Archive(id)
}

// ConsumeArchive reads one archive record and invokes handler
func (s *Store) ConsumeArchive(
	ctx context.Context, handler ArchiveHandler,
) error {
	archiver, ok := s.persistence.(Archiver)
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

func sequenceEvents(id AggregateID, atSeq int64, evs []*Event) []*Event {
	res := make([]*Event, len(evs))
	for idx, ev := range evs {
		res[idx] = &Event{
			Timestamp:   ev.Timestamp,
			Sequence:    atSeq + int64(idx),
			Type:        ev.Type,
			AggregateID: id,
			Data:        ev.Data,
			value:       ev.value,
		}
	}
	return res
}

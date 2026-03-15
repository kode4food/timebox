package timebox

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"
)

type (
	// Store persists events and snapshots
	Store struct {
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

// NewStore creates a Store using a supplied Persistence
func NewStore(p Persistence, cfg Config) (*Store, error) {
	cfg = Configure(DefaultConfig(), cfg)
	if err := cfg.validateStore(); err != nil {
		return nil, err
	}
	return newStore(cfg, p), nil
}

func newStore(cfg Config, p Persistence) *Store {
	s := &Store{
		persistence: p,
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
	if len(evs) == 0 {
		return nil
	}

	var idxs []*Index
	if s.config.Indexer != nil {
		idxs = s.config.Indexer(evs)
	}

	var status *string
	statusAt := ""
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
		statusAt = fmt.Sprintf("%d", evs[len(evs)-1].Timestamp.UnixMilli())
	}

	var re struct {
		Timestamp time.Time       `json:"timestamp"`
		Type      EventType       `json:"type"`
		Data      json.RawMessage `json:"data"`
	}
	data := make([]string, 0, len(evs))
	for _, ev := range evs {
		re.Timestamp = ev.Timestamp
		re.Type = ev.Type
		re.Data = ev.Data
		reData, err := json.Marshal(&re)
		if err != nil {
			return err
		}
		data = append(data, string(reData))
	}

	res, err := s.persistence.Append(AppendRequest{
		ID:               id,
		ExpectedSequence: atSeq,
		Status:           status,
		StatusAt:         statusAt,
		Labels:           lbls,
		Events:           data,
	})
	if err != nil {
		return err
	}
	if res != nil {
		return s.handleVersionConflict(
			id, res.NewEvents, atSeq, res.ActualSequence,
		)
	}
	return nil
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
	return s.decodeEvents(id, res.StartSequence, res.Events)
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

	events, err := s.decodeEvents(id, rec.Sequence, rec.Events)
	if err != nil {
		return nil, err
	}

	eventsSize := 0
	for _, item := range rec.Events {
		eventsSize += len(item)
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
	return s.writeSnapshot(context.Background(), id, value, sequence)
}

func (s *Store) writeSnapshot(
	ctx context.Context, id AggregateID, value any, sequence int64,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return s.persistence.SaveSnapshot(ctx, id, data, sequence)
}

func (s *Store) canSaveSnapshot() bool {
	if p, ok := s.persistence.(snapshotSaver); ok {
		return p.CanSaveSnapshot()
	}
	return true
}

// ListAggregates lists aggregate IDs that share the prefix of the provided id
func (s *Store) ListAggregates(id AggregateID) ([]AggregateID, error) {
	return s.persistence.ListAggregates(id)
}

// GetAggregateStatus returns the current indexed status for an aggregate
func (s *Store) GetAggregateStatus(id AggregateID) (string, error) {
	return s.persistence.GetAggregateStatus(id)
}

// ListAggregatesByStatus returns the aggregates currently indexed under the
// provided status, including when they entered that status
func (s *Store) ListAggregatesByStatus(status string) ([]StatusEntry, error) {
	return s.persistence.ListAggregatesByStatus(status)
}

// ListAggregatesByLabel returns the aggregates currently indexed under a
// label/value pair
func (s *Store) ListAggregatesByLabel(
	label, value string,
) ([]AggregateID, error) {
	return s.persistence.ListAggregatesByLabel(label, value)
}

// ListLabelValues returns the unique current values indexed for a label
func (s *Store) ListLabelValues(label string) ([]string, error) {
	return s.persistence.ListLabelValues(label)
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

// PollArchive reads one archive record using the provided timeout
func (s *Store) PollArchive(
	ctx context.Context, timeout time.Duration, handler ArchiveHandler,
) error {
	archiver, ok := s.persistence.(Archiver)
	if !ok {
		return ErrArchivingDisabled
	}
	return archiver.PollArchive(ctx, timeout, handler)
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf(
		"version conflict: expected sequence %d, but at %d (%d new events)",
		e.ExpectedSequence, e.ActualSequence, len(e.NewEvents),
	)
}

func (s *Store) handleVersionConflict(
	id AggregateID, rawEvents []json.RawMessage, expectedSeq, actualSeq int64,
) error {
	newEvs, err := s.decodeEvents(id, expectedSeq, rawEvents)
	if err != nil {
		return err
	}

	return &VersionConflictError{
		ExpectedSequence: expectedSeq,
		ActualSequence:   actualSeq,
		NewEvents:        newEvs,
	}
}

func (s *Store) decodeEvents(
	id AggregateID, startSeq int64, data []json.RawMessage,
) ([]*Event, error) {
	events := make([]*Event, 0, len(data))
	for i, item := range data {
		ev := &Event{}
		if err := json.Unmarshal(item, ev); err != nil {
			return nil, err
		}
		ev.Sequence = startSeq + int64(i)
		ev.AggregateID = id
		events = append(events, ev)
	}
	return events, nil
}

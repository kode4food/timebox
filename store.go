package timebox

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"
)

type (
	// Store persists, queries, and snapshots aggregate events
	Store struct {
		Queries
		backend   Backend
		schedule  *Executor[scheduleState]
		queue     *Executor[queueState]
		changes   chan struct{}
		queueWake chan struct{}
		config    Config
		changedMu sync.Mutex
		changed   map[ScheduleKey]*Schedule
	}

	// VersionConflictError is returned when AppendEvents encounters a sequence
	// mismatch. NewEvents contains the conflicting events
	VersionConflictError struct {
		NewEvents        []*Event
		ID               AggregateID
		ExpectedSequence int64
		ActualSequence   int64
	}

	// SnapshotResult holds a decoded snapshot and any events stored after it
	SnapshotResult[T any] struct {
		AdditionalEvents []*Event
		State            T
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
	s := &Store{
		Queries:   b,
		backend:   b,
		changes:   make(chan struct{}, 1),
		queueWake: make(chan struct{}, 1),
		config:    cfg,
	}
	s.schedule = s.newInternalExecutor(
		newScheduleState, scheduleAppliers.Reduce, s.notifyScheduleChange,
	)
	s.queue = s.newInternalExecutor(
		newQueueState, reduceQueue, s.notifyQueueChange,
	)
	return s, nil
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf(
		"version conflict: expected sequence %d, but at %d (%d new events)",
		e.ExpectedSequence, e.ActualSequence, len(e.NewEvents),
	)
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
		ID:      id,
		FromSeq: fromSeq,
	})
	if err != nil {
		return nil, err
	}
	if len(res.Events) == 0 {
		return []*Event{}, nil
	}
	return res.Events, nil
}

// GetSnapshot loads the latest snapshot and any events stored after it
func (s *Store) GetSnapshot[T any](
	id AggregateID,
) (*SnapshotResult[T], error) {
	rec, err := s.backend.LoadSnapshot(LoadSnapshotRequest{ID: id})
	if err != nil {
		return nil, err
	}
	state, err := decodeJSON[T](rec.Data)
	if err != nil {
		return nil, err
	}

	eventsSize := 0
	for _, ev := range rec.Events {
		eventsSize += len(ev.Data)
	}
	return &SnapshotResult[T]{
		AdditionalEvents: rec.Events,
		State:            state,
		NextSequence:     rec.Sequence,
		SnapshotSize:     len(rec.Data),
		EventsSize:       eventsSize,
	}, nil
}

// PutSnapshot saves a snapshot value and sequence if the provided sequence is
// newer than any stored snapshot
func (s *Store) PutSnapshot[T any](
	id AggregateID, value T, sequence int64,
) error {
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
	}
}

// rawAppendRequest sequences a bookkeeping append without the configured
// Indexer, which is never meant to see Timebox's own aggregates
func rawAppendRequest(id AggregateID, atSeq int64, evs []*Event) AppendRequest {
	return AppendRequest{
		ID:               id,
		ExpectedSequence: atSeq,
		Events:           sequenceEvents(id, atSeq, evs),
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

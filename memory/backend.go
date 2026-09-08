package memory

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/check"
)

type (
	// Backend keeps store state in memory for semantic tests
	Backend struct {
		timebox.AlwaysReady

		closed    bool
		nextID    int64
		aggs      map[timebox.AggregateID]*aggregate
		archive   []*timebox.ArchiveRecord
		archiveCh chan struct{}
		mu        sync.RWMutex
	}

	aggregate struct {
		id      timebox.AggregateID
		baseSeq int64
		events  []*timebox.Event

		snapshotData json.RawMessage
		snapshotSeq  int64

		status   string
		statusAt time.Time
		tags     map[string]bool
	}
)

var (
	// ErrClosed indicates the in-memory Backend has been closed
	ErrClosed = errors.New("memory backend is closed")
)

var _ timebox.Backend = (*Backend)(nil)

// Open opens a new in-memory Backend
func Open() *Backend {
	return &Backend{
		aggs:      map[timebox.AggregateID]*aggregate{},
		archive:   []*timebox.ArchiveRecord{},
		archiveCh: make(chan struct{}, 1),
	}
}

// NewStore creates a Store using the current in-memory Backend
func (b *Backend) NewStore(cfgs ...timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(b, cfgs...)
}

// Close closes the in-memory Backend
func (b *Backend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true
	b.notifyArchive()
	return nil
}

// Append appends every request's events if each expected sequence matches
func (b *Backend) Append(reqs ...timebox.AppendRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}
	if err := check.Distinct(reqs); err != nil {
		return err
	}
	for _, req := range reqs {
		if err := b.checkSequence(req); err != nil {
			return err
		}
	}
	for _, req := range reqs {
		b.applyAppend(req)
	}
	return nil
}

// LoadEvents loads events starting at fromSeq
func (b *Backend) LoadEvents(
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	a, ok := b.aggs[req.ID]
	if !ok {
		return &timebox.EventsResult{
			StartSequence: req.FromSeq,
			Events:        []*timebox.Event{},
		}, nil
	}

	start := max(req.FromSeq, a.baseSeq)
	idx := firstEventIndex(a.events, start)
	return &timebox.EventsResult{
		StartSequence: start,
		Events:        a.events[idx:],
	}, nil
}

// LoadSnapshot loads the snapshot and trailing events for an aggregate
func (b *Backend) LoadSnapshot(
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	a, ok := b.aggs[req.ID]
	if !ok {
		return &timebox.SnapshotRecord{}, nil
	}

	start := max(a.snapshotSeq-a.baseSeq, 0)
	idx := firstEventIndex(a.events, a.baseSeq+start)
	return &timebox.SnapshotRecord{
		Data:     a.snapshotData,
		Sequence: a.snapshotSeq,
		Events:   a.events[idx:],
	}, nil
}

// SaveSnapshot saves a snapshot if the sequence is not older
func (b *Backend) SaveSnapshot(req timebox.SnapshotRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}

	a := b.aggregate(req.ID)
	if req.Sequence < a.snapshotSeq {
		return nil
	}

	a.snapshotData = req.Data
	a.snapshotSeq = req.Sequence
	if req.TrimEvents && req.Sequence > a.baseSeq {
		trim := min(req.Sequence-a.baseSeq, int64(len(a.events)))
		a.events = a.events[trim:]
		a.baseSeq += trim
	}
	return nil
}

// ListAggregates lists aggregate IDs of the given type, or of every type when
// it is empty
func (b *Backend) ListAggregates(
	typ timebox.ID,
) ([]timebox.AggregateID, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.AggregateID
	for id := range b.aggs {
		if typ == "" || id.Type == typ {
			res = append(res, id)
		}
	}
	return res, nil
}

// GetAggregateStatus gets the current status for an aggregate
func (b *Backend) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return "", err
	}

	a, ok := b.aggs[id]
	if !ok {
		return "", nil
	}
	return a.status, nil
}

// ListAggregatesByStatus lists aggregates for the given status
func (b *Backend) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.StatusEntry
	for _, a := range b.aggs {
		if a.status != status {
			continue
		}
		res = append(res, timebox.StatusEntry{
			ID:        a.id,
			Timestamp: a.statusAt,
		})
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].Timestamp.Before(res[j].Timestamp)
	})
	return res, nil
}

func firstEventIndex(evs []*timebox.Event, seq int64) int {
	for i, ev := range evs {
		if ev.Sequence >= seq {
			return i
		}
	}
	return len(evs)
}

// ListAggregatesByTag lists aggregates for a tag
func (b *Backend) ListAggregatesByTag(
	tag string,
) ([]timebox.AggregateID, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.AggregateID
	for _, a := range b.aggs {
		if a.tags[tag] {
			res = append(res, a.id)
		}
	}
	return res, nil
}

// Archive archives an aggregate and removes it from active storage
func (b *Backend) Archive(id timebox.AggregateID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}
	a, ok := b.aggs[id]
	if !ok {
		return nil
	}

	b.nextID++
	rec := &timebox.ArchiveRecord{
		StreamID:         time.Now().UTC().Format(time.RFC3339Nano),
		AggregateID:      a.id,
		SnapshotData:     a.snapshotData,
		SnapshotSequence: a.snapshotSeq,
		Events:           a.events,
	}
	if b.nextID > 0 {
		rec.StreamID = rec.StreamID + "-" + time.Duration(b.nextID).String()
	}

	b.archive = append(b.archive, rec)
	delete(b.aggs, id)
	b.notifyArchive()
	return nil
}

// ConsumeArchive blocks until one archive record is available or ctx is done
func (b *Backend) ConsumeArchive(
	ctx context.Context, h timebox.ArchiveHandler,
) error {
	if h == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	for {
		rec, err := b.nextArchive()
		if err != nil {
			return err
		}
		if rec != nil {
			if err := h(ctx, rec); err != nil {
				return err
			}
			return b.consumeArchive(rec.StreamID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.archiveCh:
		}
	}
}

func (b *Backend) checkSequence(req timebox.AppendRequest) error {
	a, ok := b.aggs[req.ID]
	if !ok {
		if req.ExpectedSequence == 0 {
			return nil
		}
		return &timebox.VersionConflictError{
			ID:               req.ID,
			ExpectedSequence: req.ExpectedSequence,
		}
	}

	seq := max(a.baseSeq+int64(len(a.events)), a.snapshotSeq)
	if req.ExpectedSequence == seq {
		return nil
	}
	start := firstEventIndex(a.events, req.ExpectedSequence)
	return &timebox.VersionConflictError{
		ID:               req.ID,
		ExpectedSequence: req.ExpectedSequence,
		ActualSequence:   seq,
		NewEvents:        a.events[start:],
	}
}

func (b *Backend) applyAppend(req timebox.AppendRequest) {
	a := b.aggregate(req.ID)
	a.events = append(a.events, req.Events...)
	if req.Status != nil {
		a.status = *req.Status
		a.statusAt = req.StatusAt.UTC()
	}
	for tag, add := range req.Tags {
		if !add {
			delete(a.tags, tag)
			continue
		}
		a.tags[tag] = true
	}
}

func (b *Backend) aggregate(id timebox.AggregateID) *aggregate {
	a, ok := b.aggs[id]
	if !ok {
		a = &aggregate{
			id:     id,
			events: []*timebox.Event{},
			tags:   map[string]bool{},
		}
		b.aggs[id] = a
	}
	return a
}

func (b *Backend) nextArchive() (*timebox.ArchiveRecord, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}
	if len(b.archive) == 0 {
		return nil, nil
	}
	return b.archive[0], nil
}

func (b *Backend) consumeArchive(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}
	if len(b.archive) == 0 || b.archive[0].StreamID != id {
		return nil
	}
	b.archive = b.archive[1:]
	return nil
}

func (b *Backend) notifyArchive() {
	select {
	case b.archiveCh <- struct{}{}:
	default:
	}
}

func (b *Backend) checkClosed() error {
	if b.closed {
		return ErrClosed
	}
	return nil
}

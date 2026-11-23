package memory

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/check"
)

type (
	// Backend keeps store state in memory for semantic tests
	Backend struct {
		timebox.AlwaysReady

		publisher timebox.Publisher
		closed    bool
		aggs      map[timebox.AggregateID]*aggregate
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
		deleted  bool
	}

	// Config configures a memory Backend
	Config struct {
		Publisher timebox.Publisher
	}
)

var (
	// ErrClosed indicates the in-memory Backend has been closed
	ErrClosed = errors.New("memory backend is closed")
)

var _ timebox.Backend = (*Backend)(nil)

// Open opens a new in-memory Backend
func Open(cfgs ...Config) *Backend {
	var cfg Config
	if len(cfgs) > 0 {
		cfg = cfgs[len(cfgs)-1]
	}
	return &Backend{
		publisher: cfg.Publisher,
		aggs:      map[timebox.AggregateID]*aggregate{},
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
	return nil
}

// Append appends every request's events if each expected sequence matches
func (b *Backend) Append(reqs ...timebox.AppendRequest) error {
	if err := b.append(reqs); err != nil {
		return err
	}
	b.publisher.PublishAppends(reqs...)
	return nil
}

// LoadEvents loads events starting at fromSeq
func (b *Backend) LoadEvents(req timebox.LoadEventsRequest) (*timebox.EventsResult, error) {
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
func (b *Backend) LoadSnapshot(req timebox.LoadSnapshotRequest) (*timebox.SnapshotRecord, error) {
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

// GetAggregateStatus gets the current status for an aggregate
func (b *Backend) GetAggregateStatus(id timebox.AggregateID) (string, error) {
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

// ListAggregatesByStatus lists one page of aggregates matching the query, newest first with ties
// broken by aggregate key
func (b *Backend) ListAggregatesByStatus(q timebox.StatusQuery) ([]timebox.StatusEntry, error) {
	if q.Limit <= 0 {
		return nil, timebox.ErrInvalidLimit
	}
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.StatusEntry
	for _, a := range b.aggs {
		e := timebox.StatusEntry{
			ID:        a.id,
			Timestamp: a.statusAt,
		}
		if a.status != q.Status || a.id.Type != q.Type {
			continue
		}
		if !q.Through.IsZero() && e.Timestamp.After(q.Through) {
			continue
		}
		if q.After != nil && compareStatusEntries(e, *q.After) >= 0 {
			continue
		}
		res = append(res, e)
	}
	slices.SortFunc(res, func(l, r timebox.StatusEntry) int {
		return compareStatusEntries(r, l)
	})
	return res[:min(len(res), q.Limit)], nil
}

// GetAggregateTags loads the tags each aggregate currently holds
func (b *Backend) GetAggregateTags(
	ids ...timebox.AggregateID,
) (map[timebox.AggregateID][]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	res := map[timebox.AggregateID][]string{}
	for _, id := range ids {
		if a, ok := b.aggs[id]; ok && len(a.tags) > 0 {
			res[id] = slices.Collect(maps.Keys(a.tags))
		}
	}
	return res, nil
}

// Archive atomically bundles id's snapshot and remaining events into the shared archive queue, then
// removes id's live records
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

	record := &timebox.ArchiveRecord{
		AggregateID:      id,
		SnapshotData:     a.snapshotData,
		SnapshotSequence: a.snapshotSeq,
		Events:           a.events,
	}
	q := b.aggregate(timebox.ArchiveQueueID)
	ev, err := timebox.NewArchiveQueueEvent(q.baseSeq+int64(len(q.events)), id, record)
	if err != nil {
		return err
	}
	q.events = append(q.events, ev)
	delete(b.aggs, id)
	return nil
}

func (b *Backend) append(reqs []timebox.AppendRequest) error {
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
	if req.Delete {
		seq := req.ExpectedSequence + int64(len(req.Events))
		b.aggs[req.ID] = &aggregate{
			id:          req.ID,
			baseSeq:     seq,
			events:      []*timebox.Event{},
			snapshotSeq: seq,
			tags:        map[string]bool{},
			deleted:     true,
		}
		return
	}
	a := b.aggregate(req.ID)
	a.deleted = a.deleted && !check.Mutates(req)
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

func (b *Backend) checkClosed() error {
	if b.closed {
		return ErrClosed
	}
	return nil
}

func compareStatusEntries(l, r timebox.StatusEntry) int {
	if c := l.Timestamp.Compare(r.Timestamp); c != 0 {
		return c
	}
	return strings.Compare(string(l.ID.Key), string(r.ID.Key))
}

func firstEventIndex(evs []*timebox.Event, seq int64) int {
	for i, ev := range evs {
		if ev.Sequence >= seq {
			return i
		}
	}
	return len(evs)
}

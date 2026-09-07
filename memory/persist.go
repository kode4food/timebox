package memory

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/kode4food/timebox"
)

type (
	// Persistence keeps store state in memory for semantic tests
	Persistence struct {
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
	// ErrClosed indicates the in-memory persistence has been closed
	ErrClosed = errors.New("memory persistence is closed")
)

var _ timebox.Backend = (*Persistence)(nil)

// NewPersistence creates a new in-memory Persistence
func NewPersistence() *Persistence {
	return &Persistence{
		aggs:      map[timebox.AggregateID]*aggregate{},
		archive:   []*timebox.ArchiveRecord{},
		archiveCh: make(chan struct{}, 1),
	}
}

// NewStore creates a Store using the current in-memory Persistence
func (p *Persistence) NewStore(cfg timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(p, cfg)
}

// Close closes the in-memory Persistence
func (p *Persistence) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	p.notifyArchive()
	return nil
}

// Append appends every request's events if each expected sequence matches
func (p *Persistence) Append(reqs ...timebox.AppendRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}
	for _, req := range reqs {
		if err := p.checkSequence(req); err != nil {
			return err
		}
	}
	for _, req := range reqs {
		p.applyAppend(req)
	}
	return nil
}

// LoadEvents loads events starting at fromSeq
func (p *Persistence) LoadEvents(
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	a, ok := p.aggs[req.ID]
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
func (p *Persistence) LoadSnapshot(
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	a, ok := p.aggs[req.ID]
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
func (p *Persistence) SaveSnapshot(req timebox.SnapshotRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}

	a := p.aggregate(req.ID)
	if req.Sequence < a.snapshotSeq {
		return nil
	}

	a.snapshotData = req.Data
	a.snapshotSeq = req.Sequence
	if req.Config().TrimEvents && req.Sequence > a.baseSeq {
		trim := min(req.Sequence-a.baseSeq, int64(len(a.events)))
		a.events = a.events[trim:]
		a.baseSeq += trim
	}
	return nil
}

// ListAggregates lists aggregate IDs matching the given prefix
func (p *Persistence) ListAggregates(
	id timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.AggregateID
	for _, a := range p.aggs {
		if a.id.HasPrefix(id) {
			res = append(res, a.id)
		}
	}
	return res, nil
}

// GetAggregateStatus gets the current status for an aggregate
func (p *Persistence) GetAggregateStatus(
	id timebox.AggregateID,
) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return "", err
	}

	a, ok := p.aggs[id]
	if !ok {
		return "", nil
	}
	return a.status, nil
}

// ListAggregatesByStatus lists aggregates for the given status
func (p *Persistence) ListAggregatesByStatus(
	status string,
) ([]timebox.StatusEntry, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.StatusEntry
	for _, a := range p.aggs {
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
func (p *Persistence) ListAggregatesByTag(
	tag string,
) ([]timebox.AggregateID, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.AggregateID
	for _, a := range p.aggs {
		if a.tags[tag] {
			res = append(res, a.id)
		}
	}
	return res, nil
}

// Archive archives an aggregate and removes it from active storage
func (p *Persistence) Archive(id timebox.AggregateID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}
	a, ok := p.aggs[id]
	if !ok {
		return nil
	}

	p.nextID++
	rec := &timebox.ArchiveRecord{
		StreamID:         time.Now().UTC().Format(time.RFC3339Nano),
		AggregateID:      a.id,
		SnapshotData:     a.snapshotData,
		SnapshotSequence: a.snapshotSeq,
		Events:           a.events,
	}
	if p.nextID > 0 {
		rec.StreamID = rec.StreamID + "-" + time.Duration(p.nextID).String()
	}

	p.archive = append(p.archive, rec)
	delete(p.aggs, id)
	p.notifyArchive()
	return nil
}

// ConsumeArchive blocks until one archive record is available or ctx is done
func (p *Persistence) ConsumeArchive(
	ctx context.Context, h timebox.ArchiveHandler,
) error {
	if h == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	for {
		rec, err := p.nextArchive()
		if err != nil {
			return err
		}
		if rec != nil {
			if err := h(ctx, rec); err != nil {
				return err
			}
			return p.consumeArchive(rec.StreamID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.archiveCh:
		}
	}
}

func (p *Persistence) checkSequence(req timebox.AppendRequest) error {
	a, ok := p.aggs[req.ID]
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

func (p *Persistence) applyAppend(req timebox.AppendRequest) {
	a := p.aggregate(req.ID)
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

func (p *Persistence) aggregate(id timebox.AggregateID) *aggregate {
	a, ok := p.aggs[id]
	if !ok {
		a = &aggregate{
			id:     id,
			events: []*timebox.Event{},
			tags:   map[string]bool{},
		}
		p.aggs[id] = a
	}
	return a
}

func (p *Persistence) nextArchive() (*timebox.ArchiveRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}
	if len(p.archive) == 0 {
		return nil, nil
	}
	return p.archive[0], nil
}

func (p *Persistence) consumeArchive(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}
	if len(p.archive) == 0 || p.archive[0].StreamID != id {
		return nil
	}
	p.archive = p.archive[1:]
	return nil
}

func (p *Persistence) notifyArchive() {
	select {
	case p.archiveCh <- struct{}{}:
	default:
	}
}

func (p *Persistence) checkClosed() error {
	if p.closed {
		return ErrClosed
	}
	return nil
}

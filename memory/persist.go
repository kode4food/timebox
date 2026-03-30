package memory

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/id"
)

type (
	// Persistence keeps store state in memory for semantic tests
	Persistence struct {
		timebox.AlwaysReady
		timebox.Config

		mu        sync.RWMutex
		closed    bool
		nextID    int64
		aggs      map[string]*aggregate
		archive   []*timebox.ArchiveRecord
		archiveCh chan struct{}
	}

	aggregate struct {
		id      timebox.AggregateID
		baseSeq int64
		events  []*timebox.Event

		snapshotData json.RawMessage
		snapshotSeq  int64

		status   string
		statusAt time.Time
		labels   map[string]string
	}
)

var (
	// ErrClosed indicates the in-memory persistence has been closed
	ErrClosed = errors.New("memory persistence is closed")

	joinAggregateID, _ = id.MakeCodec('\x1f')
)

var _ timebox.Backend = (*Persistence)(nil)

// NewPersistence creates a new in-memory Persistence
func NewPersistence(cfgs ...timebox.Config) *Persistence {
	cfg := timebox.Configure(timebox.DefaultConfig(), cfgs...)
	return &Persistence{
		Config:    cfg,
		aggs:      map[string]*aggregate{},
		archive:   []*timebox.ArchiveRecord{},
		archiveCh: make(chan struct{}, 1),
	}
}

// NewStore creates a Store backed by in-memory Persistence
func NewStore(cfg timebox.Config) (*timebox.Store, error) {
	p := NewPersistence(cfg)
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

// Append appends events if the expected sequence matches
func (p *Persistence) Append(req timebox.AppendRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}

	key := keyFor(req.ID)
	a, ok := p.aggs[key]
	if !ok {
		a = &aggregate{
			id:     req.ID,
			events: []*timebox.Event{},
			labels: map[string]string{},
		}
		p.aggs[key] = a
	}

	seq := max(a.baseSeq+int64(len(a.events)), a.snapshotSeq)
	if req.ExpectedSequence != seq {
		start := min(
			max(req.ExpectedSequence-a.baseSeq, 0),
			int64(len(a.events)),
		)
		return &timebox.VersionConflictError{
			ExpectedSequence: req.ExpectedSequence,
			ActualSequence:   seq,
			NewEvents:        a.events[start:],
		}
	}

	a.events = append(a.events, req.Events...)
	if req.Status != nil {
		a.status = *req.Status
		a.statusAt = req.StatusAt.UTC()
	}
	for k, v := range req.Labels {
		if v == "" {
			delete(a.labels, k)
			continue
		}
		a.labels[k] = v
	}
	return nil
}

// LoadEvents loads events starting at fromSeq
func (p *Persistence) LoadEvents(
	id timebox.AggregateID, fromSeq int64,
) (*timebox.EventsResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	a, ok := p.aggs[keyFor(id)]
	if !ok {
		return &timebox.EventsResult{
			StartSequence: fromSeq,
			Events:        []*timebox.Event{},
		}, nil
	}

	start := max(fromSeq, a.baseSeq)
	return &timebox.EventsResult{
		StartSequence: start,
		Events:        a.events[start-a.baseSeq:],
	}, nil
}

// LoadSnapshot loads the snapshot and trailing events for an aggregate
func (p *Persistence) LoadSnapshot(
	id timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	a, ok := p.aggs[keyFor(id)]
	if !ok {
		return &timebox.SnapshotRecord{}, nil
	}

	start := max(a.snapshotSeq-a.baseSeq, 0)
	return &timebox.SnapshotRecord{
		Data:     a.snapshotData,
		Sequence: a.snapshotSeq,
		Events:   a.events[start:],
	}, nil
}

// SaveSnapshot saves a snapshot if the sequence is not older
func (p *Persistence) SaveSnapshot(
	id timebox.AggregateID, data []byte, sequence int64,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}

	key := keyFor(id)
	a, ok := p.aggs[key]
	if !ok {
		a = &aggregate{
			id:     id,
			events: []*timebox.Event{},
			labels: map[string]string{},
		}
		p.aggs[key] = a
	}
	if sequence < a.snapshotSeq {
		return nil
	}

	a.snapshotData = data
	a.snapshotSeq = sequence
	if p.TrimEvents && sequence > a.baseSeq {
		trim := min(sequence-a.baseSeq, int64(len(a.events)))
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

	a, ok := p.aggs[keyFor(id)]
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

// ListAggregatesByLabel lists aggregates for a label/value pair
func (p *Persistence) ListAggregatesByLabel(
	label, value string,
) ([]timebox.AggregateID, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	var res []timebox.AggregateID
	for _, a := range p.aggs {
		if a.labels[label] == value {
			res = append(res, a.id)
		}
	}
	return res, nil
}

// ListLabelValues lists values currently used for a label
func (p *Persistence) ListLabelValues(label string) ([]string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	for _, a := range p.aggs {
		if v := a.labels[label]; v != "" {
			seen[v] = struct{}{}
		}
	}

	res := make([]string, 0, len(seen))
	for v := range seen {
		res = append(res, v)
	}
	sort.Strings(res)
	return res, nil
}

// Archive archives an aggregate and removes it from active storage
func (p *Persistence) Archive(id timebox.AggregateID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}
	key := keyFor(id)
	a, ok := p.aggs[key]
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
	delete(p.aggs, key)
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

func keyFor(id timebox.AggregateID) string {
	return joinAggregateID(id)
}

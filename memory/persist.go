package memory

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kode4food/timebox"
)

type (
	// Persistence keeps store state in memory for semantic tests
	Persistence struct {
		mu        sync.RWMutex
		cfg       timebox.Config
		closed    bool
		nextID    int64
		aggs      map[string]*aggregate
		archive   []*timebox.ArchiveRecord
		archiveCh chan struct{}
	}

	aggregate struct {
		id      timebox.AggregateID
		baseSeq int64
		events  []json.RawMessage

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
)

var _ timebox.Persistence = (*Persistence)(nil)

// NewPersistence creates a new in-memory Persistence
func NewPersistence(cfgs ...timebox.Config) *Persistence {
	cfg := timebox.Configure(timebox.DefaultConfig(), cfgs...)
	return &Persistence{
		cfg:       cfg,
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

// Ready reports that in-memory persistence can serve requests immediately
func (p *Persistence) Ready() <-chan struct{} {
	return timebox.ReadyNow()
}

// Append appends events if the expected sequence matches
func (p *Persistence) Append(
	req timebox.AppendRequest,
) (*timebox.AppendResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return nil, err
	}

	key := keyFor(req.ID)
	a, ok := p.aggs[key]
	if !ok {
		a = &aggregate{
			id:     slices.Clone(req.ID),
			events: []json.RawMessage{},
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
		return &timebox.AppendResult{
			ActualSequence: seq,
			NewEvents:      cloneMessages(a.events[start:]),
		}, nil
	}

	for _, e := range req.Events {
		a.events = append(a.events, json.RawMessage(e))
	}
	if req.Status != nil {
		a.status = *req.Status
		if ts, err := strconv.ParseInt(req.StatusAt, 10, 64); err == nil {
			a.statusAt = time.UnixMilli(ts).UTC()
		}
	}
	for k, v := range req.Labels {
		if v == "" {
			delete(a.labels, k)
			continue
		}
		a.labels[k] = v
	}
	return nil, nil
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
			Events:        []json.RawMessage{},
		}, nil
	}

	start := max(fromSeq, a.baseSeq)
	return &timebox.EventsResult{
		StartSequence: start,
		Events:        cloneMessages(a.events[start-a.baseSeq:]),
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
		Data:     cloneMessage(a.snapshotData),
		Sequence: a.snapshotSeq,
		Events:   cloneMessages(a.events[start:]),
	}, nil
}

// SaveSnapshot saves a snapshot if the sequence is not older
func (p *Persistence) SaveSnapshot(
	ctx context.Context, id timebox.AggregateID, data []byte, sequence int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkClosed(); err != nil {
		return err
	}

	key := keyFor(id)
	a, ok := p.aggs[key]
	if !ok {
		a = &aggregate{
			id:     slices.Clone(id),
			events: []json.RawMessage{},
			labels: map[string]string{},
		}
		p.aggs[key] = a
	}
	if sequence < a.snapshotSeq {
		return nil
	}

	a.snapshotData = append(json.RawMessage{}, data...)
	a.snapshotSeq = sequence
	if p.cfg.Snapshot.TrimEvents && sequence > a.baseSeq {
		trim := min(sequence-a.baseSeq, int64(len(a.events)))
		a.events = cloneMessages(a.events[trim:])
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
			res = append(res, slices.Clone(a.id))
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
			ID:        slices.Clone(a.id),
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
			res = append(res, slices.Clone(a.id))
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
	if !p.cfg.Archiving {
		return timebox.ErrArchivingDisabled
	}

	key := keyFor(id)
	a, ok := p.aggs[key]
	if !ok {
		return nil
	}

	p.nextID++
	rec := &timebox.ArchiveRecord{
		StreamID:         time.Now().UTC().Format(time.RFC3339Nano),
		AggregateID:      slices.Clone(a.id),
		SnapshotData:     cloneMessage(a.snapshotData),
		SnapshotSequence: a.snapshotSeq,
		Events:           cloneMessages(a.events),
	}
	if p.nextID > 0 {
		rec.StreamID = rec.StreamID + "-" + time.Duration(p.nextID).String()
	}

	p.archive = append(p.archive, rec)
	delete(p.aggs, key)
	p.notifyArchive()
	return nil
}

// ConsumeArchive blocks until an archive record is available
func (p *Persistence) ConsumeArchive(
	ctx context.Context, h timebox.ArchiveHandler,
) error {
	return p.PollArchive(ctx, 0, h)
}

// PollArchive waits up to timeout for one archive record
func (p *Persistence) PollArchive(
	ctx context.Context, timeout time.Duration, h timebox.ArchiveHandler,
) error {
	if !p.cfg.Archiving {
		return timebox.ErrArchivingDisabled
	}
	if h == nil {
		return timebox.ErrArchiveHandlerMissing
	}

	wait := (<-chan time.Time)(nil)
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		wait = t.C
	}

	for {
		rec, err := p.nextArchive()
		if err != nil {
			return err
		}
		if rec != nil {
			if err := h(ctx, cloneArchive(rec)); err != nil {
				return err
			}
			return p.consumeArchive(rec.StreamID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wait:
			return nil
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

func cloneArchive(r *timebox.ArchiveRecord) *timebox.ArchiveRecord {
	return &timebox.ArchiveRecord{
		StreamID:         r.StreamID,
		AggregateID:      slices.Clone(r.AggregateID),
		SnapshotData:     cloneMessage(r.SnapshotData),
		SnapshotSequence: r.SnapshotSequence,
		Events:           cloneMessages(r.Events),
	}
}

func cloneMessages(in []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(in))
	for _, m := range in {
		out = append(out, cloneMessage(m))
	}
	return out
}

func cloneMessage(in json.RawMessage) json.RawMessage {
	if len(in) == 0 {
		return nil
	}
	return append(json.RawMessage{}, in...)
}

func keyFor(id timebox.AggregateID) string {
	return id.Join("\x1f")
}

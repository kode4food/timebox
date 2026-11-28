package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/check"
)

type (
	mutation struct {
		Appends  []appendRecord
		Snapshot *timebox.SnapshotRequest
		Archive  *timebox.AggregateID
	}

	appendRecord struct {
		ID       timebox.AggregateID
		Expected int64
		Events   []eventRecord
		Status   *string
		StatusAt time.Time
		Tags     map[string]bool
		Delete   bool
		Delivery []delivery
	}

	eventRecord struct {
		At        time.Time
		Type      timebox.EventType
		Data      []byte
		Sequence  int64
		ArchiveID string `json:",omitempty"`
	}

	conflict struct {
		ID       timebox.AggregateID
		Expected int64
		Actual   int64
		Events   []eventRecord
	}
)

// Append applies all requests in one PostgreSQL transaction on the DBOS system database
func (b *Backend) Append(reqs ...timebox.AppendRequest) error {
	if err := b.checkOpen(); err != nil {
		return err
	}
	if err := check.Distinct(reqs); err != nil {
		return err
	}
	if len(reqs) == 0 {
		return nil
	}
	input := mutation{Appends: make([]appendRecord, 0, len(reqs))}
	for _, req := range reqs {
		record := appendRecord{
			ID:       req.ID,
			Expected: req.ExpectedSequence,
			Events:   make([]eventRecord, 0, len(req.Events)),
			Status:   req.Status,
			StatusAt: req.StatusAt,
			Tags:     req.Tags,
			Delete:   req.Delete,
			Delivery: deliveries(&req),
		}
		for i, ev := range req.Events {
			record.Events = append(record.Events, eventRecord{
				At:       ev.Timestamp,
				Type:     ev.Type,
				Data:     ev.Data,
				Sequence: req.ExpectedSequence + int64(i),
			})
		}
		input.Appends = append(input.Appends, record)
	}
	if err := b.write(input); err != nil {
		return err
	}
	b.cfg.Publisher.PublishAppends(reqs...)
	return nil
}

func (b *Backend) write(input mutation) error {
	if err := b.checkOpen(); err != nil {
		return err
	}
	conflict, err := b.commitWrite(&input)
	if err != nil || conflict == nil {
		return err
	}
	events, err := b.materializeEvents(conflict.ID, conflict.Events)
	if err != nil {
		return err
	}
	return &timebox.VersionConflictError{
		ID:               conflict.ID,
		ExpectedSequence: conflict.Expected,
		ActualSequence:   conflict.Actual,
		NewEvents:        events,
	}
}

// Another process's records surface as a journal ID collision, so writes skip the upfront refresh
func (b *Backend) commitWrite(input *mutation) (*conflict, error) {
	v := b.lockJournals(input.aggregateIDs())
	defer v.unlock()
	for refreshed := false; ; refreshed = true {
		if refreshed {
			if err := b.refresh(v); err != nil {
				return nil, err
			}
		}
		res, wrote, err := b.tryCommit(v, input)
		if errors.Is(err, dbos.ErrWorkflowIDInUse) {
			continue
		}
		if err != nil || wrote || refreshed {
			return res, err
		}
	}
}

func (b *Backend) tryCommit(v *journalView, input *mutation) (*conflict, bool, error) {
	ctx := context.Background()
	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res, records, err := b.commit(tx, v, input)
	if err != nil || len(records) == 0 {
		return res, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return nil, true, applyRecords(v, records)
}

func (b *Backend) commit(
	tx pgx.Tx, v *journalView, input *mutation,
) (*conflict, []journalEntry, error) {
	entry, res, err := b.prepareEntry(tx, v, input)
	if err != nil || entry == nil {
		return res, nil, err
	}
	records, err := b.enqueueJournal(tx, v, entry)
	return res, records, err
}

func (b *Backend) prepareEntry(
	tx pgx.Tx, v *journalView, input *mutation,
) (*journalEntry, *conflict, error) {
	entry := &journalEntry{Mutation: input}
	switch {
	case input.Archive != nil:
		target := v.streams[*input.Archive]
		if target == nil || !target.live {
			return nil, nil, nil
		}
		id, err := b.enqueueArchive(tx, *input.Archive, target)
		if err != nil {
			return nil, nil, err
		}
		entry.ArchiveID = id
	case input.Snapshot != nil:
		req := input.Snapshot
		if s := v.streams[req.ID]; s != nil && req.Sequence < s.snapSeq {
			return nil, nil, nil
		}
	default:
		if c := v.checkAppends(input.Appends); c != nil {
			return nil, c, nil
		}
		mutates, err := b.enqueueAppends(tx, v, input.Appends)
		if err != nil || !mutates {
			return nil, nil, err
		}
	}
	return entry, nil, nil
}

func (b *Backend) enqueueAppends(tx pgx.Tx, v *journalView, reqs []appendRecord) (bool, error) {
	mutates := false
	for i := range reqs {
		req := &reqs[i]
		if !req.mutates() {
			continue
		}
		mutates = true
		if req.ID == timebox.ArchiveQueueID {
			if err := b.ackArchives(tx, v.archives, req); err != nil {
				return false, err
			}
		}
		for _, work := range req.Delivery {
			if err := b.enqueueDelivery(tx, work); err != nil {
				return false, err
			}
		}
	}
	return mutates, nil
}

// The JSON round trip leaves local state identical to what a refresh decodes
func applyRecords(v *journalView, records []journalEntry) error {
	for i, j := range v.logs {
		raw, err := json.Marshal(records[i])
		if err != nil {
			return err
		}
		var e journalEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		if err := v.applyRecord(j, &e); err != nil {
			return err
		}
	}
	return nil
}

func (m *mutation) aggregateIDs() []timebox.AggregateID {
	switch {
	case m.Archive != nil:
		return []timebox.AggregateID{*m.Archive, timebox.ArchiveQueueID}
	case m.Snapshot != nil:
		return []timebox.AggregateID{m.Snapshot.ID}
	default:
		ids := make([]timebox.AggregateID, 0, len(m.Appends))
		for _, req := range m.Appends {
			ids = append(ids, req.ID)
		}
		return ids
	}
}

func (r *appendRecord) mutates() bool {
	return r.Delete || len(r.Events) != 0 || r.Status != nil || len(r.Tags) != 0
}

func (e *eventRecord) event(id timebox.AggregateID) *timebox.Event {
	return &timebox.Event{
		Timestamp:   e.At.UTC(),
		Sequence:    e.Sequence,
		AggregateID: id,
		Type:        e.Type,
		Data:        slices.Clone(e.Data),
	}
}

func toEvents(id timebox.AggregateID, records []eventRecord) []*timebox.Event {
	if len(records) == 0 {
		return nil
	}
	evs := make([]*timebox.Event, 0, len(records))
	for i := range records {
		evs = append(evs, records[i].event(id))
	}
	return evs
}

package dbos

import (
	"slices"

	"github.com/kode4food/timebox"
)

// SaveSnapshot persists a snapshot and optionally trims covered events atomically
func (b *Backend) SaveSnapshot(req timebox.SnapshotRequest) error {
	if req.ID == timebox.ArchiveQueueID {
		req.Data = nil
	}
	return b.write(mutation{Snapshot: &req})
}

// LoadEvents loads an aggregate's events and effective starting sequence
func (b *Backend) LoadEvents(req timebox.LoadEventsRequest) (*timebox.EventsResult, error) {
	v := b.lockJournals([]timebox.AggregateID{req.ID})
	defer v.unlock()
	if err := b.refresh(v); err != nil {
		return nil, err
	}

	res := &timebox.EventsResult{StartSequence: req.FromSeq}
	if s := v.streams[req.ID]; s != nil {
		res.StartSequence = max(s.base, req.FromSeq)
		events, err := b.materializeEvents(req.ID, s.from(res.StartSequence))
		if err != nil {
			return nil, err
		}
		res.Events = events
	}
	return res, nil
}

// LoadSnapshot loads a snapshot and events at or after its sequence
func (b *Backend) LoadSnapshot(req timebox.LoadSnapshotRequest) (*timebox.SnapshotRecord, error) {
	v := b.lockJournals([]timebox.AggregateID{req.ID})
	defer v.unlock()
	if err := b.refresh(v); err != nil {
		return nil, err
	}

	if s := v.streams[req.ID]; s != nil {
		data := slices.Clone(s.snapshot)
		if req.ID == timebox.ArchiveQueueID && s.snapSeq != 0 {
			var err error
			data, err = b.materializeArchiveSnapshot(v.archiveSnapshot)
			if err != nil {
				return nil, err
			}
		}
		events, err := b.materializeEvents(req.ID, s.from(s.snapSeq))
		if err != nil {
			return nil, err
		}
		return &timebox.SnapshotRecord{
			Data:     data,
			Events:   events,
			Sequence: s.snapSeq,
		}, nil
	}
	return &timebox.SnapshotRecord{}, nil
}

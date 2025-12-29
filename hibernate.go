package timebox

import (
	"context"
	"encoding/json"
	"errors"
)

type (
	// Hibernator persists aggregate data for explicit archiving
	Hibernator interface {
		Get(context.Context, AggregateID) (*HibernateRecord, error)
		Put(context.Context, AggregateID, *HibernateRecord) error
		Delete(context.Context, AggregateID) error
	}

	// HibernateRecord stores the snapshots and event list for an aggregate
	HibernateRecord struct {
		Events    []json.RawMessage         `json:"events"`
		Snapshots map[string]SnapshotRecord `json:"snapshots"`
	}

	// SnapshotRecord stores a snapshot payload and its sequence
	SnapshotRecord struct {
		Data     json.RawMessage `json:"data"`
		Sequence int64           `json:"sequence"`
	}
)

var (
	// ErrNoHibernator indicates no Hibernator was configured on the Store
	ErrNoHibernator = errors.New("no hibernator configured")

	// ErrHibernateNotFound indicates a hibernated aggregate was not found
	ErrHibernateNotFound = errors.New("hibernated aggregate not found")
)

var (
	emptyEvents = []*Event{}

	emptyHibernate = &HibernateRecord{
		Events:    []json.RawMessage{},
		Snapshots: map[string]SnapshotRecord{},
	}

	emptySnapshot = &SnapshotResult{
		AdditionalEvents: emptyEvents,
		NextSequence:     0,
		ShouldSnapshot:   false,
	}
)

// Hibernate archives the aggregate by storing its snapshot and event log
// through the configured Hibernator, then removing the Redis keys
func (s *Store) Hibernate(ctx context.Context, id AggregateID) error {
	if s.hibernator == nil {
		return ErrNoHibernator
	}

	snapKey := s.buildKey(id, snapshotValSuffix)
	snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{snapKey, snapSeqKey, eventsKey}

	result, err := s.getHibernate.Run(ctx, s.client, keys).Result()
	if err != nil {
		return err
	}

	record, err := s.buildHibernateRecord(result, defaultSnapshot)
	if err != nil {
		return err
	}
	if record == emptyHibernate {
		return nil
	}

	if err := s.hibernator.Put(ctx, id, record); err != nil {
		return err
	}

	_, err = s.client.Del(ctx, eventsKey, snapKey, snapSeqKey).Result()
	return err
}

func (s *Store) loadHibernatedEvents(
	ctx context.Context, id AggregateID, fromSeq int64,
) ([]*Event, error) {
	record, err := s.hibernator.Get(ctx, id)
	if errors.Is(err, ErrHibernateNotFound) {
		return emptyEvents, nil
	}
	if err != nil {
		return nil, err
	}
	if len(record.Events) == 0 || fromSeq >= int64(len(record.Events)) {
		return emptyEvents, nil
	}

	start := int(fromSeq)
	return s.decodeEvents(id, fromSeq, record.Events[start:])
}

func (s *Store) loadHibernatedSnapshot(
	ctx context.Context, id AggregateID, snapshotName string, target any,
) (*SnapshotResult, error) {
	record, err := s.hibernator.Get(ctx, id)
	if errors.Is(err, ErrHibernateNotFound) {
		return emptySnapshot, nil
	}
	if err != nil {
		return nil, err
	}

	snapSeq := int64(0)
	snapSize := 0
	snap, ok := record.Snapshots[snapshotName]
	if ok {
		snapSeq = snap.Sequence
		snapSize = len(snap.Data)
	}
	if ok && len(snap.Data) > 0 {
		if err := json.Unmarshal(snap.Data, target); err != nil {
			return nil, err
		}
	}
	if snapSeq < 0 || snapSeq > int64(len(record.Events)) {
		return nil, ErrUnexpectedLuaResult
	}

	start := int(snapSeq)
	events, err := s.decodeEvents(id, snapSeq, record.Events[start:])
	if err != nil {
		return nil, err
	}

	eventsSize := 0
	for _, ev := range record.Events[start:] {
		eventsSize += len(ev)
	}

	return &SnapshotResult{
		AdditionalEvents: events,
		NextSequence:     snapSeq,
		ShouldSnapshot:   eventsSize > snapSize,
	}, nil
}

func (s *Store) buildHibernateRecord(
	result any, snapshotName string,
) (*HibernateRecord, error) {
	resultSlice := result.([]any)
	if len(resultSlice) < 3 {
		return nil, ErrUnexpectedLuaResult
	}

	snapData := resultSlice[0].(string)
	snapSeq := resultSlice[1].(int64)
	rawEvents := resultSlice[2].([]any)

	if snapData == "" && len(rawEvents) == 0 {
		return emptyHibernate, nil
	}

	rawMessages, err := toRawMessages(rawEvents)
	if err != nil {
		return nil, err
	}

	record := &HibernateRecord{
		Events:    make([]json.RawMessage, 0, len(rawMessages)),
		Snapshots: map[string]SnapshotRecord{},
	}
	if snapData != "" || snapSeq > 0 {
		record.Snapshots[snapshotName] = SnapshotRecord{
			Data:     json.RawMessage(snapData),
			Sequence: snapSeq,
		}
	}
	record.Events = append(record.Events, rawMessages...)

	return record, nil
}

package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kode4food/caravan/topic"
	"github.com/redis/go-redis/v9"
)

type (
	// Store persists events and snapshots in Redis and publishes appended
	// events to the Timebox EventHub
	Store struct {
		tb             *Timebox
		client         *redis.Client
		prefix         string
		producer       topic.Producer[*Event]
		appendEvents   *redis.Script
		getEvents      *redis.Script
		putSnapshot    *redis.Script
		getSnapshot    *redis.Script
		publishArchive *redis.Script
		consumeArchive *redis.Script
		snapshotWorker *SnapshotWorker
		config         StoreConfig
	}

	// VersionConflictError is returned when AppendEvents encounters a sequence
	// mismatch. NewEvents contains the conflicting events.
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
)

const (
	// RedisConnectTimeout is the ping timeout when creating a Store
	RedisConnectTimeout = 5 * time.Second

	eventsSuffix = ":events"

	defaultSnapshot   = "snapshot"
	snapshotValSuffix = ":" + defaultSnapshot + ":val"
	snapshotSeqSuffix = ":" + defaultSnapshot + ":seq"
)

var (
	// ErrUnexpectedLuaResult indicates a Lua script returned data in an
	// unexpected shape
	ErrUnexpectedLuaResult = errors.New("unexpected result from Lua script")
)

// NewStore creates a new Store instance backed by Redis that publishes events
// to the Timebox event hub
func (tb *Timebox) NewStore(cfg StoreConfig) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	pingCtx, cancel := context.WithTimeout(tb.ctx, RedisConnectTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, err
	}

	appendEventsScript := luaAppendEvents
	getEventsScript := luaGetEvents
	putSnapshotScript := luaPutSnapshot
	getSnapshotScript := luaGetSnapshot
	if cfg.TrimEvents {
		appendEventsScript = luaAppendEventsTrim
		getEventsScript = luaGetEventsTrim
		putSnapshotScript = luaPutSnapshotTrim
		getSnapshotScript = luaGetSnapshotTrim
	}

	s := &Store{
		tb:             tb,
		client:         client,
		prefix:         cfg.Prefix,
		producer:       tb.hub.newProducer(),
		appendEvents:   redis.NewScript(appendEventsScript),
		getEvents:      redis.NewScript(getEventsScript),
		putSnapshot:    redis.NewScript(putSnapshotScript),
		getSnapshot:    redis.NewScript(getSnapshotScript),
		publishArchive: redis.NewScript(luaPublishArchive),
		consumeArchive: redis.NewScript(luaConsumeArchive),
		config:         cfg,
	}

	if tb.config.Workers && cfg.WorkerCount > 0 {
		s.snapshotWorker = NewSnapshotWorker(s, cfg)
	}
	return s, nil
}

// Close stops background workers, closes the event producer, and releases the
// Redis client
func (s *Store) Close() error {
	if s.snapshotWorker != nil {
		s.snapshotWorker.Stop()
	}
	if s.producer != nil {
		s.producer.Close()
	}
	return s.client.Close()
}

// AppendEvents atomically appends events for an aggregate if the expected
// sequence matches the current log sequence, publishing them to the EventHub
func (s *Store) AppendEvents(
	ctx context.Context, id AggregateID, atSeq int64, evs []*Event,
) error {
	if len(evs) == 0 {
		return nil
	}

	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	if s.config.TrimEvents {
		snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
		keys = []string{eventsKey, snapSeqKey}
	}
	args := []any{atSeq}

	var re struct {
		Timestamp time.Time       `json:"timestamp"`
		Type      EventType       `json:"type"`
		Data      json.RawMessage `json:"data"`
	}
	for _, ev := range evs {
		re.Timestamp = ev.Timestamp
		re.Type = ev.Type
		re.Data = ev.Data
		reData, err := json.Marshal(&re)
		if err != nil {
			return err
		}
		args = append(args, string(reData))
	}

	result, err := s.appendEvents.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return err
	}

	res := result.([]any)
	success := res[0].(int64)
	seq := res[1].(int64)

	if success == 0 {
		newEvents := res[2].([]any)
		return s.handleVersionConflict(id, newEvents, atSeq, seq)
	}

	if s.producer != nil {
		for _, ev := range evs {
			if s.tb.hub.hasSubscribers(ev.Type, ev.AggregateID) {
				s.producer.Send() <- ev
			}
		}
	}

	return nil
}

// GetEvents returns all events for an aggregate starting at fromSeq
func (s *Store) GetEvents(
	ctx context.Context, id AggregateID, fromSeq int64,
) ([]*Event, error) {
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	if s.config.TrimEvents {
		snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
		keys = []string{eventsKey, snapSeqKey}
	}
	args := []any{fromSeq}

	result, err := s.getEvents.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}

	if s.config.TrimEvents {
		res := result.([]any)
		if len(res) < 2 {
			return nil, ErrUnexpectedLuaResult
		}

		offset, ok := res[0].(int64)
		if !ok {
			return nil, ErrUnexpectedLuaResult
		}

		rawMessages, err := toRawMessages(res[1].([]any))
		if err != nil {
			return nil, err
		}
		if len(rawMessages) > 0 {
			startSeq := fromSeq
			if startSeq < offset {
				startSeq = offset
			}
			return s.decodeEvents(id, startSeq, rawMessages)
		}

		return []*Event{}, nil
	}

	rawMessages, err := toRawMessages(result.([]any))
	if err != nil {
		return nil, err
	}
	if len(rawMessages) > 0 {
		return s.decodeEvents(id, fromSeq, rawMessages)
	}

	return []*Event{}, nil
}

// GetSnapshot loads the latest snapshot into target and returns any events
// stored after the snapshot sequence
func (s *Store) GetSnapshot(
	ctx context.Context, id AggregateID, target any,
) (*SnapshotResult, error) {
	snapKey := s.buildKey(id, snapshotValSuffix)
	snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{snapKey, snapSeqKey, eventsKey}

	result, err := s.getSnapshot.Run(ctx, s.client, keys).Result()
	if err != nil {
		return nil, err
	}

	resultSlice := result.([]any)
	if len(resultSlice) < 3 {
		return nil, ErrUnexpectedLuaResult
	}

	snapData := resultSlice[0].(string)
	snapSize := len(snapData)
	snapSeq := resultSlice[1].(int64)
	newEvents := resultSlice[2].([]any)

	if snapData != "" {
		if err := json.Unmarshal([]byte(snapData), target); err != nil {
			return nil, err
		}
	}

	newMessages, err := toRawMessages(newEvents)
	if err != nil {
		return nil, err
	}
	events, err := s.decodeEvents(id, snapSeq, newMessages)
	if err != nil {
		return nil, err
	}

	eventsSize := 0
	for i := range events {
		eventsSize += len(newMessages[i])
	}

	return &SnapshotResult{
		AdditionalEvents: events,
		NextSequence:     snapSeq,
		ShouldSnapshot:   eventsSize > snapSize,
	}, nil
}

// PutSnapshot saves a snapshot value and sequence if the provided sequence is
// newer than any stored snapshot
func (s *Store) PutSnapshot(
	ctx context.Context, id AggregateID, value any, sequence int64,
) error {
	snapKey := s.buildKey(id, snapshotValSuffix)
	snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	keys := []string{snapKey, snapSeqKey}
	if s.config.TrimEvents {
		eventsKey := s.buildKey(id, eventsSuffix)
		keys = []string{snapKey, snapSeqKey, eventsKey}
	}

	_, err = s.putSnapshot.Run(ctx, s.client, keys, string(data), sequence).Result()
	return err
}

// ListAggregates lists aggregate IDs that share the prefix of the provided id
func (s *Store) ListAggregates(
	ctx context.Context, id AggregateID,
) ([]AggregateID, error) {
	str := id.Join(":")
	searchKeys := []string{
		fmt.Sprintf("%s:%s%s", s.prefix, str, eventsSuffix),
		fmt.Sprintf("%s:%s%s", s.prefix, str, snapshotSeqSuffix),
	}

	seen := map[string]AggregateID{}
	for _, searchKey := range searchKeys {
		keys, err := s.client.Keys(ctx, searchKey).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			trimmed := strings.TrimPrefix(key, s.prefix+":")
			aggregateIDStr := strings.TrimSuffix(trimmed, eventsSuffix)
			aggregateIDStr = strings.TrimSuffix(aggregateIDStr, snapshotSeqSuffix)
			aid := s.parseAggregateID(aggregateIDStr)
			seen[aggregateIDStr] = aid
		}
	}

	ids := make([]AggregateID, 0, len(seen))
	for _, aid := range seen {
		ids = append(ids, aid)
	}

	return ids, nil
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf(
		"version conflict: expected sequence %d, but at %d (%d new events)",
		e.ExpectedSequence, e.ActualSequence, len(e.NewEvents),
	)
}

func (s *Store) buildKey(id AggregateID, suffix string) string {
	str := id.Join(":")
	return fmt.Sprintf("%s:%s%s", s.prefix, str, suffix)
}

func (s *Store) archiveStreamKey() string {
	return fmt.Sprintf("%s:archive", s.prefix)
}

func (s *Store) archiveGroup() string {
	return fmt.Sprintf("%s:archive:group", s.prefix)
}

func (s *Store) archiveConsumer() string {
	return fmt.Sprintf("%s:archive:consumer", s.prefix)
}

func (s *Store) handleVersionConflict(
	id AggregateID, rawEvents []any, expectedSeq, actualSeq int64,
) error {
	rawMessages, err := toRawMessages(rawEvents)
	if err != nil {
		return err
	}
	newEvs, err := s.decodeEvents(id, expectedSeq, rawMessages)
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

func (s *Store) parseAggregateID(str string) AggregateID {
	return ParseAggregateID(str, ":")
}

func toRawMessages(data []any) ([]json.RawMessage, error) {
	messages := make([]json.RawMessage, 0, len(data))
	for _, item := range data {
		str, ok := item.(string)
		if !ok {
			return nil, ErrUnexpectedLuaResult
		}
		messages = append(messages, json.RawMessage(str))
	}
	return messages, nil
}

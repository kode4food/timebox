package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/kode4food/caravan/topic"
)

type (
	Store struct {
		tb              *Timebox
		client          *redis.Client
		prefix          string
		producer        topic.Producer[*Event]
		appendEventsLua *redis.Script
		getEventsLua    *redis.Script
		putSnapshotLua  *redis.Script
		getSnapshotLua  *redis.Script
		snapshotWorker  *SnapshotWorker
		config          StoreConfig
	}

	VersionConflictError struct {
		NewEvents        []*Event
		ExpectedSequence int64
		ActualSequence   int64
	}

	SnapshotResult struct {
		AdditionalEvents []*Event
		NextSequence     int64
		ShouldSnapshot   bool
	}
)

const (
	RedisConnectTimeout = 5 * time.Second

	eventsSuffix      = ":events"
	snapshotValSuffix = ":snapshot:val"
	snapshotSeqSuffix = ":snapshot:seq"
)

var (
	ErrUnexpectedLuaResult = errors.New("unexpected result from Lua script")
)

// NewStore creates a new Store instance that publishes events to the Timebox
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

	s := &Store{
		tb:              tb,
		client:          client,
		prefix:          cfg.Prefix,
		producer:        tb.hub.NewProducer(),
		appendEventsLua: redis.NewScript(luaAppendEvents),
		getEventsLua:    redis.NewScript(luaGetEvents),
		putSnapshotLua:  redis.NewScript(luaPutSnapshot),
		getSnapshotLua:  redis.NewScript(luaGetSnapshot),
		config:          cfg,
	}

	if tb.config.EnableSnapshotWorker {
		s.snapshotWorker = NewSnapshotWorker(s, cfg)
	}
	return s, nil
}

func (s *Store) Close() error {
	if s.snapshotWorker != nil {
		s.snapshotWorker.Stop()
	}
	if s.producer != nil {
		s.producer.Close()
	}
	return s.client.Close()
}

func (s *Store) AppendEvents(
	ctx context.Context, id AggregateID, atSeq int64, evs []*Event,
) error {
	if len(evs) == 0 {
		return nil
	}

	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	args := []any{atSeq}

	for _, ev := range evs {
		eventData, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		args = append(args, string(eventData))
	}

	result, err := s.appendEventsLua.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return err
	}

	res := result.([]any)
	success := res[0].(int64)
	seq := res[1].(int64)

	if success == 0 {
		return s.handleVersionConflict(res[2:], atSeq, seq)
	}

	if s.producer != nil {
		for _, ev := range evs {
			s.producer.Send() <- ev
		}
	}

	return nil
}

func (s *Store) GetEvents(
	ctx context.Context, id AggregateID, fromSeq int64,
) ([]*Event, error) {
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	args := []any{fromSeq}

	result, err := s.getEventsLua.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}

	return s.unmarshalEvents(fromSeq, result.([]any))
}

func (s *Store) GetSnapshot(
	ctx context.Context, id AggregateID, target any,
) (*SnapshotResult, error) {
	snapKey := s.buildKey(id, snapshotValSuffix)
	snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{snapKey, snapSeqKey, eventsKey}

	result, err := s.getSnapshotLua.Run(ctx, s.client, keys).Result()
	if err != nil {
		return nil, err
	}

	resultSlice := result.([]any)
	if len(resultSlice) < 2 {
		return nil, ErrUnexpectedLuaResult
	}

	snapData := resultSlice[0].(string)
	snapSize := len(snapData)
	snapSeq := resultSlice[1].(int64)

	if snapData != "" {
		if err := json.Unmarshal([]byte(snapData), target); err != nil {
			return nil, err
		}
	}

	events, err := s.unmarshalEvents(snapSeq, resultSlice[2:])
	if err != nil {
		return nil, err
	}

	eventsSize := 0
	for i := range events {
		eventsSize += len(resultSlice[i+2].(string))
	}

	return &SnapshotResult{
		AdditionalEvents: events,
		NextSequence:     snapSeq,
		ShouldSnapshot:   eventsSize > snapSize,
	}, nil
}

func (s *Store) PutSnapshot(
	ctx context.Context, id AggregateID, value any, sequence int64,
) error {
	snapKey := s.buildKey(id, snapshotValSuffix)
	snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	_, err = s.putSnapshotLua.Run(
		ctx, s.client,
		[]string{snapKey, snapSeqKey},
		string(data), sequence,
	).Result()
	return err
}

func (s *Store) ListAggregates(
	ctx context.Context, id AggregateID,
) ([]AggregateID, error) {
	str := id.Join(":")
	searchKey := fmt.Sprintf("%s:%s%s", s.prefix, str, eventsSuffix)

	keys, err := s.client.Keys(ctx, searchKey).Result()
	if err != nil {
		return nil, err
	}

	var ids []AggregateID
	for _, key := range keys {
		trimmed := strings.TrimPrefix(key, s.prefix+":")
		aggregateIDStr := strings.TrimSuffix(trimmed, eventsSuffix)
		aid := s.parseAggregateID(aggregateIDStr)
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

func (s *Store) handleVersionConflict(
	rawEvents []any, expectedSeq, actualSeq int64,
) error {
	newEvs, err := s.unmarshalEvents(expectedSeq, rawEvents)
	if err != nil {
		return err
	}

	return &VersionConflictError{
		ExpectedSequence: expectedSeq,
		ActualSequence:   actualSeq,
		NewEvents:        newEvs,
	}
}

func (s *Store) buildKey(id AggregateID, suffix string) string {
	str := id.Join(":")
	return fmt.Sprintf("%s:%s%s", s.prefix, str, suffix)
}

func (s *Store) parseAggregateID(str string) AggregateID {
	return ParseAggregateID(str, ":")
}

func (s *Store) unmarshalEvents(startSeq int64, data []any) ([]*Event, error) {
	events := make([]*Event, 0, len(data))
	for i, item := range data {
		ev := &Event{}
		if err := json.Unmarshal([]byte(item.(string)), ev); err != nil {
			return nil, err
		}
		ev.Sequence = startSeq + int64(i)
		events = append(events, ev)
	}
	return events, nil
}

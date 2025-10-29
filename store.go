package timebox

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/kode4food/caravan/topic"
)

type (
	Store interface {
		io.Closer

		AppendEvents(
			ctx context.Context, id AggregateID, events []*Event,
		) error

		GetEvents(
			ctx context.Context, id AggregateID, fromSeq int64,
		) ([]*Event, error)

		GetSnapshot(
			ctx context.Context, id AggregateID, target any,
		) (events []*Event, shouldSnapshot bool, err error)

		PutSnapshot(
			ctx context.Context, id AggregateID, value any, sequence int64,
		) error

		ListAggregates(
			ctx context.Context, keyPattern AggregateID,
		) ([]AggregateID, error)
	}

	store struct {
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
)

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf(
		"version conflict: expected sequence %d, but at %d (%d new events)",
		e.ExpectedSequence, e.ActualSequence, len(e.NewEvents),
	)
}

const (
	RedisConnectTimeout = 5 * time.Second

	eventsSuffix      = ":events"
	snapshotValSuffix = ":snapshot:val"
	snapshotSeqSuffix = ":snapshot:seq"
)

func NewStore(hub EventHub, cfg StoreConfig) (Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(
		context.Background(), RedisConnectTimeout,
	)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	s := &store{
		client:          client,
		prefix:          cfg.Prefix,
		producer:        hub.NewProducer(),
		appendEventsLua: redis.NewScript(luaAppendEvents),
		getEventsLua:    redis.NewScript(luaGetEvents),
		putSnapshotLua:  redis.NewScript(luaPutSnapshot),
		getSnapshotLua:  redis.NewScript(luaGetSnapshot),
		config:          cfg,
	}

	s.snapshotWorker = NewSnapshotWorker(s, cfg)
	return s, nil
}

func (s *store) Close() error {
	if s.snapshotWorker != nil {
		s.snapshotWorker.Stop()
	}
	if s.producer != nil {
		s.producer.Close()
	}
	return s.client.Close()
}

func (s *store) AppendEvents(
	ctx context.Context, id AggregateID, evs []*Event,
) error {
	if len(evs) == 0 {
		return nil
	}

	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	args := []any{evs[0].Sequence}

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
		return s.handleVersionConflict(res[2:], evs[0].Sequence, seq)
	}

	if s.producer != nil {
		for _, ev := range evs {
			s.producer.Send() <- ev
		}
	}

	return nil
}

func (s *store) handleVersionConflict(
	rawEvents []any, expectedSeq, actualSeq int64,
) error {
	newEvs, err := s.unmarshalEvents(rawEvents)
	if err != nil {
		return err
	}

	return &VersionConflictError{
		ExpectedSequence: expectedSeq,
		ActualSequence:   actualSeq,
		NewEvents:        newEvs,
	}
}

func (s *store) GetEvents(
	ctx context.Context, id AggregateID, fromSeq int64,
) ([]*Event, error) {
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	args := []any{fromSeq}

	result, err := s.getEventsLua.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}

	return s.unmarshalEvents(result.([]any))
}

func (s *store) GetSnapshot(
	ctx context.Context, id AggregateID, target any,
) ([]*Event, bool, error) {
	snapKey := s.buildKey(id, snapshotValSuffix)
	snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{snapKey, snapSeqKey, eventsKey}

	result, err := s.getSnapshotLua.Run(ctx, s.client, keys).Result()
	if err != nil {
		return nil, false, err
	}

	resultSlice := result.([]any)
	if len(resultSlice) < 2 {
		return nil, false, fmt.Errorf(
			"unexpected result format from Lua script",
		)
	}

	snapData := resultSlice[0].(string)
	snapSize := len(snapData)

	if snapData != "" {
		if err := json.Unmarshal([]byte(snapData), target); err != nil {
			return nil, false, err
		}
	}

	events, err := s.unmarshalEvents(resultSlice[2:])
	if err != nil {
		return nil, false, err
	}

	eventsSize := 0
	for i := range events {
		eventsSize += len(resultSlice[i+2].(string))
	}

	return events, eventsSize > snapSize, nil
}

func (s *store) PutSnapshot(
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

func (s *store) ListAggregates(
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

func (s *store) buildKey(id AggregateID, suffix string) string {
	str := id.Join(":")
	return fmt.Sprintf("%s:%s%s", s.prefix, str, suffix)
}

func (s *store) parseAggregateID(str string) AggregateID {
	return ParseAggregateID(str, ":")
}

func (s *store) unmarshalEvents(data []any) ([]*Event, error) {
	events := make([]*Event, 0, len(data))
	for _, item := range data {
		ev := &Event{}
		if err := json.Unmarshal([]byte(item.(string)), ev); err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, nil
}

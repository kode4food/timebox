package timebox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type (
	// Store persists events and snapshots in Redis
	Store struct {
		client         *redis.Client
		prefix         string
		appendScripts  map[luaAppendSpec]*redis.Script
		getEvents      *redis.Script
		putSnapshot    *redis.Script
		getSnapshot    *redis.Script
		publishArchive *redis.Script
		consumeArchive *redis.Script
		snapshotWorker *SnapshotWorker
		config         Config
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

	eventsSuffix = "events"

	idxPrefix    = "idx"
	statusSuffix = idxPrefix + ":status"
	labelsSuffix = idxPrefix + ":labels"
	labelSuffix  = idxPrefix + ":label"

	defaultSnapshot   = "snapshot"
	snapshotValSuffix = defaultSnapshot + ":val"
	snapshotSeqSuffix = defaultSnapshot + ":seq"

	archiveStreamSuffix   = "archive"
	archiveGroupSuffix    = archiveStreamSuffix + ":group"
	archiveConsumerSuffix = archiveStreamSuffix + ":consumer"
)

var (
	// ErrUnexpectedLuaResult indicates a Lua script returned data in an
	// unexpected shape
	ErrUnexpectedLuaResult = errors.New("unexpected result from Lua script")
)

// NewStore creates a new Store instance backed by Redis
func NewStore(cfgs ...Config) (*Store, error) {
	cfg := DefaultConfig().With(cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	pingCtx, cancel := context.WithTimeout(context.Background(), RedisConnectTimeout)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, err
	}

	getEventsScript := luaGetEvents
	putSnapshotScript := luaPutSnapshot
	getSnapshotScript := luaGetSnapshot
	if cfg.Snapshot.TrimEvents {
		getEventsScript = luaGetEventsTrim
		putSnapshotScript = luaPutSnapshotTrim
		getSnapshotScript = luaGetSnapshotTrim
	}

	s := &Store{
		client:         client,
		prefix:         buildStorePrefix(cfg),
		appendScripts:  makeLuaAppendScripts(),
		getEvents:      redis.NewScript(getEventsScript),
		putSnapshot:    redis.NewScript(putSnapshotScript),
		getSnapshot:    redis.NewScript(getSnapshotScript),
		publishArchive: redis.NewScript(luaPublishArchive),
		consumeArchive: redis.NewScript(luaConsumeArchive),
		config:         cfg,
	}

	if cfg.Snapshot.Workers {
		s.snapshotWorker = NewSnapshotWorker(s)
	}
	return s, nil
}

// Close stops background workers and releases the Redis client
func (s *Store) Close() error {
	if s.snapshotWorker != nil {
		s.snapshotWorker.Stop()
	}
	return s.client.Close()
}

// AppendEvents atomically appends events for an aggregate if the expected
// sequence matches the current log sequence
func (s *Store) AppendEvents(
	ctx context.Context, id AggregateID, atSeq int64, evs []*Event,
) error {
	if len(evs) == 0 {
		return nil
	}

	var idxs []*Index
	if s.config.Indexer != nil {
		idxs = s.config.Indexer(evs)
	}

	var status *string
	statusAt := ""
	lbls := map[string]string{}
	for _, idx := range idxs {
		if idx != nil && idx.Status != nil {
			status = idx.Status
		}
		if idx != nil {
			maps.Copy(lbls, idx.Labels)
		}
	}
	if status != nil && len(evs) > 0 {
		statusAt = fmt.Sprintf("%d", evs[len(evs)-1].Timestamp.UnixMilli())
	}

	var re struct {
		Timestamp time.Time       `json:"timestamp"`
		Type      EventType       `json:"type"`
		Data      json.RawMessage `json:"data"`
	}
	data := make([]string, 0, len(evs))
	for _, ev := range evs {
		re.Timestamp = ev.Timestamp
		re.Type = ev.Type
		re.Data = ev.Data
		reData, err := json.Marshal(&re)
		if err != nil {
			return err
		}
		data = append(data, string(reData))
	}

	call := buildLuaAppendCall(s, luaAppendInput{
		id:       id,
		atSeq:    atSeq,
		status:   status,
		statusAt: statusAt,
		labels:   lbls,
		events:   data,
	})

	result, err := s.appendScripts[call.spec].Run(
		ctx, s.client, call.keys, call.args...,
	).Result()
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

	return nil
}

// GetEvents returns all events for an aggregate starting at fromSeq
func (s *Store) GetEvents(
	ctx context.Context, id AggregateID, fromSeq int64,
) ([]*Event, error) {
	eventsKey := s.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	if s.config.Snapshot.TrimEvents {
		snapSeqKey := s.buildKey(id, snapshotSeqSuffix)
		keys = []string{eventsKey, snapSeqKey}
	}
	args := []any{fromSeq}

	result, err := s.getEvents.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return nil, err
	}

	if s.config.Snapshot.TrimEvents {
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
			startSeq := max(fromSeq, offset)
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
	if s.config.Snapshot.TrimEvents {
		eventsKey := s.buildKey(id, eventsSuffix)
		keys = []string{snapKey, snapSeqKey, eventsKey}
	}

	_, err = s.putSnapshot.Run(
		ctx, s.client, keys, string(data), sequence,
	).Result()
	return err
}

// ListAggregates lists aggregate IDs that share the prefix of the provided id
func (s *Store) ListAggregates(
	ctx context.Context, id AggregateID,
) ([]AggregateID, error) {
	searchKeys := []string{
		s.buildKey(id, eventsSuffix),
		s.buildKey(id, snapshotSeqSuffix),
	}

	seen := map[string]AggregateID{}
	for _, searchKey := range searchKeys {
		keys, err := s.client.Keys(ctx, searchKey).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			aid := s.parseAggregateIDFromKey(key)
			seen[aid.Join(":")] = aid
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
	return fmt.Sprintf("%s:%s:%s", s.prefix, s.config.Redis.JoinKey(id), suffix)
}

func (s *Store) buildGlobalKey(suffix string) string {
	return fmt.Sprintf("%s:%s", s.prefix, suffix)
}

func (s *Store) buildLabelStateKey(id AggregateID) string {
	return s.buildGlobalKey(
		fmt.Sprintf("%s:%s", labelsSuffix, s.config.Redis.JoinKey(id)),
	)
}

func (s *Store) buildLabelRootKey() string {
	return s.buildGlobalKey(labelSuffix)
}

func (s *Store) archiveStreamKey() string {
	return s.buildGlobalKey(archiveStreamSuffix)
}

func (s *Store) archiveGroup() string {
	return s.buildGlobalKey(archiveGroupSuffix)
}

func (s *Store) archiveConsumer() string {
	return s.buildGlobalKey(archiveConsumerSuffix)
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

func (s *Store) parseAggregateIDFromKey(key string) AggregateID {
	str, _ := strings.CutPrefix(key, s.prefix+":")

	for _, suffix := range [...]string{
		":" + eventsSuffix,
		":" + snapshotValSuffix,
		":" + snapshotSeqSuffix,
	} {
		if trimmed, ok := strings.CutSuffix(str, suffix); ok {
			str = trimmed
			break
		}
	}

	return s.config.Redis.ParseKey(str)
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

func buildStorePrefix(cfg Config) string {
	if cfg.Redis.Shard == "" {
		return cfg.Redis.Prefix
	}
	if cfg.Redis.Prefix == "" {
		return "{" + cfg.Redis.Shard + "}"
	}
	return fmt.Sprintf("%s:{%s}", cfg.Redis.Prefix, cfg.Redis.Shard)
}

func escapeKeyPart(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	return strings.ReplaceAll(s, ":", `%`)
}

func unescapeKeyPart(s string) string {
	i := 0
	for i < len(s) && s[i] != '\\' && s[i] != '%' {
		i++
	}
	if i == len(s) {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))
	b.WriteString(s[:i])

	for i < len(s) {
		switch s[i] {
		case '%':
			b.WriteByte(':')
			i++
		case '\\':
			i++
			if i >= len(s) {
				b.WriteByte('\\')
				break
			}
			b.WriteByte(s[i])
			i++
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String()
}

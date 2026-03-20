package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kode4food/timebox"
)

// Persistence implements timebox.Persistence using Redis/Valkey
type Persistence struct {
	timebox.AlwaysReady
	Config

	client         *redis.Client
	prefix         string
	appendScripts  map[luaAppendSpec]*redis.Script
	getEvents      *redis.Script
	putSnapshot    *redis.Script
	getSnapshot    *redis.Script
	publishArchive *redis.Script
	consumeArchive *redis.Script
}

const (
	connectTimeout = 5 * time.Second

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

// ErrUnexpectedLuaResult indicates a Redis Lua script returned data in an
// unexpected shape
var ErrUnexpectedLuaResult = errors.New("unexpected result from Lua script")

// NewStore creates a Store backed by Redis persistence
func NewStore(cfgs ...Config) (*timebox.Store, error) {
	p, err := NewPersistence(cfgs...)
	if err != nil {
		return nil, err
	}
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	return timebox.NewStore(p, cfg.Timebox)
}

// NewPersistence creates Redis-backed Persistence
func NewPersistence(cfgs ...Config) (*Persistence, error) {
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newPersistence(cfg)
}

func newPersistence(cfg Config) (*Persistence, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	pingCtx, cancel := context.WithTimeout(
		context.Background(), connectTimeout,
	)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, err
	}

	getEventsScript := luaGetEvents
	putSnapshotScript := luaPutSnapshot
	getSnapshotScript := luaGetSnapshot
	if cfg.Timebox.Snapshot.TrimEvents {
		getEventsScript = luaGetEventsTrim
		putSnapshotScript = luaPutSnapshotTrim
		getSnapshotScript = luaGetSnapshotTrim
	}

	return &Persistence{
		client:         client,
		prefix:         buildStorePrefix(cfg),
		appendScripts:  makeLuaAppendScripts(),
		getEvents:      redis.NewScript(getEventsScript),
		putSnapshot:    redis.NewScript(putSnapshotScript),
		getSnapshot:    redis.NewScript(getSnapshotScript),
		publishArchive: redis.NewScript(luaPublishArchive),
		consumeArchive: redis.NewScript(luaConsumeArchive),
		Config:         cfg,
	}, nil
}

// Close closes the Redis client
func (p *Persistence) Close() error {
	return p.client.Close()
}

// Append appends events if the expected sequence matches
func (p *Persistence) Append(
	req timebox.AppendRequest,
) (*timebox.AppendResult, error) {
	evs, err := p.encodeEvents(req.Events)
	if err != nil {
		return nil, err
	}

	call := buildLuaAppendCall(p, luaAppendInput{
		id:       req.ID,
		atSeq:    req.ExpectedSequence,
		status:   req.Status,
		statusAt: req.StatusAt,
		labels:   req.Labels,
		events:   evs,
	})

	result, err := p.appendScripts[call.spec].Run(
		context.Background(), p.client, call.keys, call.args...,
	).Result()
	if err != nil {
		return nil, err
	}

	res := result.([]any)
	success := res[0].(int64)
	seq := res[1].(int64)
	if success != 0 {
		return nil, nil
	}

	newEvents, err := p.decodeEvents(res[2].([]any))
	if err != nil {
		return nil, err
	}
	return &timebox.AppendResult{
		ActualSequence: seq,
		NewEvents:      newEvents,
	}, nil
}

// LoadEvents loads events starting at fromSeq
func (p *Persistence) LoadEvents(
	id timebox.AggregateID, fromSeq int64,
) (*timebox.EventsResult, error) {
	eventsKey := p.buildKey(id, eventsSuffix)
	keys := []string{eventsKey}
	if p.Timebox.Snapshot.TrimEvents {
		snapSeqKey := p.buildKey(id, snapshotSeqSuffix)
		keys = []string{eventsKey, snapSeqKey}
	}
	args := []any{fromSeq}

	result, err := p.getEvents.Run(
		context.Background(), p.client, keys, args...,
	).Result()
	if err != nil {
		return nil, err
	}

	if p.Timebox.Snapshot.TrimEvents {
		res := result.([]any)
		if len(res) < 2 {
			return nil, errors.Join(
				timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
			)
		}

		offset, ok := res[0].(int64)
		if !ok {
			return nil, errors.Join(
				timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
			)
		}

		start := max(fromSeq, offset)
		evs, err := p.decodeEvents(res[1].([]any))
		if err != nil {
			return nil, err
		}
		return &timebox.EventsResult{
			StartSequence: start,
			Events:        evs,
		}, nil
	}

	evs, err := p.decodeEvents(result.([]any))
	if err != nil {
		return nil, err
	}
	return &timebox.EventsResult{
		StartSequence: fromSeq,
		Events:        evs,
	}, nil
}

// LoadSnapshot loads the snapshot and trailing events for an aggregate
func (p *Persistence) LoadSnapshot(
	id timebox.AggregateID,
) (*timebox.SnapshotRecord, error) {
	snapKey := p.buildKey(id, snapshotValSuffix)
	snapSeqKey := p.buildKey(id, snapshotSeqSuffix)
	eventsKey := p.buildKey(id, eventsSuffix)
	keys := []string{snapKey, snapSeqKey, eventsKey}

	result, err := p.getSnapshot.Run(
		context.Background(), p.client, keys,
	).Result()
	if err != nil {
		return nil, err
	}

	resultSlice := result.([]any)
	if len(resultSlice) < 3 {
		return nil, errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}

	snapData, ok := resultSlice[0].(string)
	if !ok {
		return nil, errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}

	snapSeq, ok := resultSlice[1].(int64)
	if !ok {
		return nil, errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}

	newEvents, err := p.decodeEvents(resultSlice[2].([]any))
	if err != nil {
		return nil, err
	}

	return &timebox.SnapshotRecord{
		Data:     json.RawMessage(snapData),
		Sequence: snapSeq,
		Events:   newEvents,
	}, nil
}

// SaveSnapshot saves a snapshot if the provided sequence is not older
func (p *Persistence) SaveSnapshot(
	id timebox.AggregateID, data []byte, sequence int64,
) error {
	snapKey := p.buildKey(id, snapshotValSuffix)
	snapSeqKey := p.buildKey(id, snapshotSeqSuffix)
	keys := []string{snapKey, snapSeqKey}
	if p.Timebox.Snapshot.TrimEvents {
		eventsKey := p.buildKey(id, eventsSuffix)
		keys = []string{snapKey, snapSeqKey, eventsKey}
	}

	_, err := p.putSnapshot.Run(
		context.Background(), p.client, keys, string(data), sequence,
	).Result()
	return err
}

// ListAggregates lists aggregate IDs matching the given prefix
func (p *Persistence) ListAggregates(
	id timebox.AggregateID,
) ([]timebox.AggregateID, error) {
	searchKeys := []string{
		p.buildKey(id, eventsSuffix),
		p.buildKey(id, snapshotSeqSuffix),
	}

	seen := map[string]timebox.AggregateID{}
	for _, searchKey := range searchKeys {
		keys, err := p.client.Keys(context.Background(), searchKey).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			aid := p.parseAggregateIDFromKey(key)
			seen[aid.Join(":")] = aid
		}
	}

	ids := make([]timebox.AggregateID, 0, len(seen))
	for _, aid := range seen {
		ids = append(ids, aid)
	}
	return ids, nil
}

func (p *Persistence) buildKey(id timebox.AggregateID, suffix string) string {
	return fmt.Sprintf("%s:%s:%s", p.prefix, p.JoinKey(id), suffix)
}

func (p *Persistence) buildLabelStateKey(id timebox.AggregateID) string {
	return fmt.Sprintf("%s:%s:%s", p.prefix, p.JoinKey(id), labelsSuffix)
}

func (p *Persistence) buildLabelRootKey() string {
	return fmt.Sprintf("%s:%s", p.prefix, labelSuffix)
}

func (p *Persistence) archiveStreamKey() string {
	return fmt.Sprintf("%s:%s", p.prefix, archiveStreamSuffix)
}

func (p *Persistence) archiveGroup() string {
	return fmt.Sprintf("%s:%s", p.prefix, archiveGroupSuffix)
}

func (p *Persistence) archiveConsumer() string {
	return fmt.Sprintf("%s:%s", p.prefix, archiveConsumerSuffix)
}

func (p *Persistence) parseAggregateIDFromKey(key string) timebox.AggregateID {
	str, _ := strings.CutPrefix(key, p.prefix+":")

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

	return p.ParseKey(str)
}

func (p *Persistence) encodeEvents(evs []*timebox.Event) ([]string, error) {
	return timebox.EncodeAll(timebox.JSONEvent, evs)
}

func (p *Persistence) decodeEvents(data []any) ([]*timebox.Event, error) {
	vals := make([]string, 0, len(data))
	for _, item := range data {
		str, ok := item.(string)
		if !ok {
			return nil, timebox.ErrUnexpectedResult
		}
		vals = append(vals, str)
	}
	return timebox.DecodeAll(timebox.JSONEvent, vals)
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

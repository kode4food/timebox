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
	"github.com/kode4food/timebox/internal/check"
)

// Persistence implements timebox.Persistence using Redis/Valkey
type Persistence struct {
	timebox.AlwaysReady
	Config

	client         *redis.Client
	prefix         string
	appendScript   *redis.Script
	getEvents      map[bool]*redis.Script
	putSnapshot    map[bool]*redis.Script
	getSnapshot    map[bool]*redis.Script
	publishArchive *redis.Script
	consumeArchive *redis.Script
}

const (
	connectTimeout = 5 * time.Second

	eventsSuffix = "events"

	idxPrefix    = "idx"
	statusSuffix = idxPrefix + ":status"
	tagsSuffix   = idxPrefix + ":tags"
	tagSuffix    = idxPrefix + ":tag"

	defaultSnapshot   = "snapshot"
	snapshotValSuffix = defaultSnapshot + ":val"
	snapshotSeqSuffix = defaultSnapshot + ":seq"

	archiveStreamSuffix   = "archive"
	archiveGroupSuffix    = archiveStreamSuffix + ":group"
	archiveConsumerSuffix = archiveStreamSuffix + ":consumer"
)

var (
	// ErrUnexpectedLuaResult indicates a Redis Lua script returned data in an
	// unexpected shape
	ErrUnexpectedLuaResult = errors.New("unexpected result from Lua script")
)

var _ timebox.Backend = (*Persistence)(nil)

// NewPersistence creates Redis-backed Persistence
func NewPersistence(cfgs ...Config) (*Persistence, error) {
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newPersistence(cfg)
}

// NewStore creates a Store using the current Redis Persistence
func (p *Persistence) NewStore(cfg timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(p, cfg)
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

	return &Persistence{
		client:       client,
		prefix:       buildStorePrefix(cfg),
		appendScript: redis.NewScript(luaAppend),
		getEvents: map[bool]*redis.Script{
			false: redis.NewScript(luaGetEvents),
			true:  redis.NewScript(luaGetEventsTrim),
		},
		putSnapshot: map[bool]*redis.Script{
			false: redis.NewScript(luaPutSnapshot),
			true:  redis.NewScript(luaPutSnapshotTrim),
		},
		getSnapshot: map[bool]*redis.Script{
			false: redis.NewScript(luaGetSnapshot),
			true:  redis.NewScript(luaGetSnapshotTrim),
		},
		publishArchive: redis.NewScript(luaPublishArchive),
		consumeArchive: redis.NewScript(luaConsumeArchive),
		Config:         cfg,
	}, nil
}

// Close closes the Redis client
func (p *Persistence) Close() error {
	return p.client.Close()
}

// Append appends every request's events if each expected sequence matches
func (p *Persistence) Append(reqs ...timebox.AppendRequest) error {
	if len(reqs) == 0 {
		return nil
	}
	if err := check.Distinct(reqs); err != nil {
		return err
	}

	var keys []string
	args := []any{len(reqs)}
	for _, req := range reqs {
		evs, err := timebox.EncodeJSONEvents(req.Events)
		if err != nil {
			return err
		}
		keys, args = p.appendLuaCall(keys, args, req.Store, luaAppendInput{
			id:       req.ID,
			atSeq:    req.ExpectedSequence,
			status:   req.Status,
			statusAt: req.StatusAt,
			tags:     req.Tags,
			events:   evs,
		})
	}

	result, err := p.appendScript.Run(
		context.Background(), p.client, keys, args...,
	).Result()
	if err != nil {
		return err
	}
	return appendConflict(reqs, result)
}

// LoadEvents loads events starting at fromSeq
func (p *Persistence) LoadEvents(
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	trimEvents := req.Config().TrimEvents
	eventsKey := p.buildKey(req.ID, eventsSuffix)
	keys := []string{eventsKey}
	if trimEvents {
		keys = append(keys, p.buildKey(req.ID, snapshotSeqSuffix))
	}
	args := []any{req.FromSeq}

	result, err := p.getEvents[trimEvents].Run(
		context.Background(), p.client, keys, args...,
	).Result()
	if err != nil {
		return nil, err
	}

	if trimEvents {
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

		evs, err := decodeEvents(res[1].([]any))
		if err != nil {
			return nil, err
		}
		return &timebox.EventsResult{
			StartSequence: max(req.FromSeq, offset),
			Events:        evs,
		}, nil
	}

	evs, err := decodeEvents(result.([]any))
	if err != nil {
		return nil, err
	}
	return &timebox.EventsResult{
		StartSequence: req.FromSeq,
		Events:        evs,
	}, nil
}

// LoadSnapshot loads the snapshot and trailing events for an aggregate
func (p *Persistence) LoadSnapshot(
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	snapKey := p.buildKey(req.ID, snapshotValSuffix)
	snapSeqKey := p.buildKey(req.ID, snapshotSeqSuffix)
	eventsKey := p.buildKey(req.ID, eventsSuffix)
	keys := []string{snapKey, snapSeqKey, eventsKey}

	result, err := p.getSnapshot[req.Config().TrimEvents].Run(
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

	newEvents, err := decodeEvents(resultSlice[2].([]any))
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
func (p *Persistence) SaveSnapshot(req timebox.SnapshotRequest) error {
	trimEvents := req.Config().TrimEvents
	snapKey := p.buildKey(req.ID, snapshotValSuffix)
	snapSeqKey := p.buildKey(req.ID, snapshotSeqSuffix)
	keys := []string{snapKey, snapSeqKey}
	if trimEvents {
		keys = []string{snapKey, snapSeqKey, p.buildKey(req.ID, eventsSuffix)}
	}

	_, err := p.putSnapshot[trimEvents].Run(
		context.Background(), p.client, keys, string(req.Data), req.Sequence,
	).Result()
	return err
}

// ListAggregates lists aggregate IDs of the given type, or of every type when
// it is empty
func (p *Persistence) ListAggregates(
	typ timebox.ID,
) ([]timebox.AggregateID, error) {
	searchKeys := []string{
		p.listAggregateKey(eventsSuffix),
		p.listAggregateKey(snapshotSeqSuffix),
	}

	seen := map[timebox.AggregateID]struct{}{}
	for _, searchKey := range searchKeys {
		keys, err := p.client.Keys(context.Background(), searchKey).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			aid := p.parseAggregateIDFromKey(key)
			if typ == "" || aid.Type == typ {
				seen[aid] = struct{}{}
			}
		}
	}

	ids := make([]timebox.AggregateID, 0, len(seen))
	for aid := range seen {
		ids = append(ids, aid)
	}
	return ids, nil
}

func (p *Persistence) listAggregateKey(suffix string) string {
	return fmt.Sprintf("%s:*:%s", p.prefix, suffix)
}

func (p *Persistence) buildKey(id timebox.AggregateID, suffix string) string {
	return fmt.Sprintf("%s:%s:%s", p.prefix, joinAggregateID(id), suffix)
}

func (p *Persistence) buildTagStateKey(id timebox.AggregateID) string {
	return fmt.Sprintf("%s:%s:%s", p.prefix, joinAggregateID(id), tagsSuffix)
}

func (p *Persistence) buildTagRootKey() string {
	return fmt.Sprintf("%s:%s", p.prefix, tagSuffix)
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

	return parseAggregateID(str)
}

// appendConflict turns the script result into a VersionConflictError naming the
// request that failed, or nil when every append landed
func appendConflict(reqs []timebox.AppendRequest, result any) error {
	res, ok := result.([]any)
	if !ok || len(res) < 4 {
		return errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}
	success, ok := res[0].(int64)
	if !ok {
		return errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}
	if success != 0 {
		return nil
	}

	seq, seqOK := res[1].(int64)
	at, atOK := res[3].(int64)
	if !seqOK || !atOK || at < 1 || at > int64(len(reqs)) {
		return errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}
	raw, ok := res[2].([]any)
	if !ok {
		return errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}
	newEvents, err := decodeEvents(raw)
	if err != nil {
		return err
	}

	req := reqs[at-1]
	return &timebox.VersionConflictError{
		ID:               req.ID,
		ExpectedSequence: req.ExpectedSequence,
		ActualSequence:   seq,
		NewEvents:        newEvents,
	}
}

func escapeKeyPart(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	return strings.ReplaceAll(s, ":", `%`)
}

func decodeEvents(items []any) ([]*timebox.Event, error) {
	res := make([]*timebox.Event, 0, len(items))
	for _, item := range items {
		s, ok := item.(string)
		if !ok {
			return nil, timebox.ErrUnexpectedResult
		}
		ev, err := timebox.JSONEvent.Decode([]byte(s))
		if err != nil {
			return nil, err
		}
		res = append(res, ev)
	}
	return res, nil
}

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

// Backend implements timebox.Backend using Redis/Valkey
type Backend struct {
	timebox.AlwaysReady
	cfg Config

	client       *redis.Client
	prefix       string
	appendScript *redis.Script
	getEvents    *redis.Script
	putSnapshot  *redis.Script
	getSnapshot  *redis.Script
	archive      *redis.Script
}

const (
	connectTimeout = 5 * time.Second

	eventsSuffix = "events"
	baseSuffix   = "base"

	idxPrefix    = "idx"
	statusSuffix = idxPrefix + ":status"
	tagsSuffix   = idxPrefix + ":tags"
	tagSuffix    = idxPrefix + ":tag"

	defaultSnapshot   = "snapshot"
	snapshotValSuffix = defaultSnapshot + ":val"
	snapshotSeqSuffix = defaultSnapshot + ":seq"
)

var (
	// ErrUnexpectedLuaResult indicates a Redis Lua script returned data in an
	// unexpected shape
	ErrUnexpectedLuaResult = errors.New("unexpected result from Lua script")
)

var _ timebox.Backend = (*Backend)(nil)

// Open opens Redis-backed Backend
func Open(cfgs ...Config) (*Backend, error) {
	cfg := timebox.Configure(DefaultConfig(), cfgs...)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return newBackend(cfg)
}

// NewStore creates a Store using the current Redis Backend
func (b *Backend) NewStore(cfgs ...timebox.Config) (*timebox.Store, error) {
	return timebox.NewStore(b, cfgs...)
}

func newBackend(cfg Config) (*Backend, error) {
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

	return &Backend{
		cfg:          cfg,
		client:       client,
		prefix:       buildStorePrefix(cfg),
		appendScript: redis.NewScript(luaAppend),
		getEvents:    redis.NewScript(luaGetEvents),
		putSnapshot:  redis.NewScript(luaPutSnapshot),
		getSnapshot:  redis.NewScript(luaGetSnapshot),
		archive:      redis.NewScript(luaArchive),
	}, nil
}

// Close closes the Redis client
func (b *Backend) Close() error {
	return b.client.Close()
}

// Append appends every request's events if each expected sequence matches
func (b *Backend) Append(reqs ...timebox.AppendRequest) error {
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
		keys, args = b.appendLuaCall(keys, args, luaAppendInput{
			id:       req.ID,
			atSeq:    req.ExpectedSequence,
			status:   req.Status,
			statusAt: req.StatusAt,
			tags:     req.Tags,
			events:   evs,
			delete:   req.Delete,
		})
	}

	result, err := b.appendScript.Run(
		context.Background(), b.client, keys, args...,
	).Result()
	if err != nil {
		return err
	}
	if err := appendConflict(reqs, result); err != nil {
		return err
	}
	b.cfg.Publisher.PublishAppends(reqs...)
	return nil
}

// LoadEvents loads events starting at fromSeq
func (b *Backend) LoadEvents(
	req timebox.LoadEventsRequest,
) (*timebox.EventsResult, error) {
	keys := []string{
		b.buildKey(req.ID, eventsSuffix), b.buildKey(req.ID, baseSuffix),
	}

	result, err := b.getEvents.Run(
		context.Background(), b.client, keys, req.FromSeq,
	).Result()
	if err != nil {
		return nil, err
	}

	res, ok := result.([]any)
	if !ok || len(res) < 2 {
		return nil, errors.Join(
			timebox.ErrUnexpectedResult, ErrUnexpectedLuaResult,
		)
	}

	base, ok := res[0].(int64)
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
		StartSequence: max(req.FromSeq, base),
		Events:        evs,
	}, nil
}

// LoadSnapshot loads the snapshot and trailing events for an aggregate
func (b *Backend) LoadSnapshot(
	req timebox.LoadSnapshotRequest,
) (*timebox.SnapshotRecord, error) {
	snapKey := b.buildKey(req.ID, snapshotValSuffix)
	snapSeqKey := b.buildKey(req.ID, snapshotSeqSuffix)
	eventsKey := b.buildKey(req.ID, eventsSuffix)
	baseKey := b.buildKey(req.ID, baseSuffix)
	keys := []string{snapKey, snapSeqKey, eventsKey, baseKey}

	result, err := b.getSnapshot.Run(
		context.Background(), b.client, keys,
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
func (b *Backend) SaveSnapshot(req timebox.SnapshotRequest) error {
	keys := []string{
		b.buildKey(req.ID, snapshotValSuffix),
		b.buildKey(req.ID, snapshotSeqSuffix),
		b.buildKey(req.ID, eventsSuffix),
		b.buildKey(req.ID, baseSuffix),
	}
	trim := "0"
	if req.TrimEvents {
		trim = "1"
	}

	_, err := b.putSnapshot.Run(
		context.Background(), b.client, keys,
		string(req.Data), req.Sequence, trim,
	).Result()
	return err
}

// ListAggregates lists aggregate IDs of the given type, or of every type when
// it is empty
func (b *Backend) ListAggregates(
	typ timebox.ID,
) ([]timebox.AggregateID, error) {
	searchKeys := []string{
		b.listAggregateKey(eventsSuffix),
		b.listAggregateKey(snapshotValSuffix),
	}

	seen := map[timebox.AggregateID]struct{}{}
	for _, searchKey := range searchKeys {
		keys, err := b.client.Keys(context.Background(), searchKey).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			aid := b.parseAggregateIDFromKey(key)
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

func (b *Backend) listAggregateKey(suffix string) string {
	return fmt.Sprintf("%s:*:%s", b.prefix, suffix)
}

func (b *Backend) buildKey(id timebox.AggregateID, suffix string) string {
	return fmt.Sprintf("%s:%s:%s", b.prefix, joinAggregateID(id), suffix)
}

func (b *Backend) buildTagStateKey(id timebox.AggregateID) string {
	return fmt.Sprintf("%s:%s:%s", b.prefix, joinAggregateID(id), tagsSuffix)
}

func (b *Backend) buildTagRootKey() string {
	return fmt.Sprintf("%s:%s", b.prefix, tagSuffix)
}

func (b *Backend) parseAggregateIDFromKey(key string) timebox.AggregateID {
	str, _ := strings.CutPrefix(key, b.prefix+":")

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

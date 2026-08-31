package redis

import (
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/kode4food/timebox"
)

type (
	luaAppendBuilder struct {
		out         strings.Builder
		spec        luaAppendSpec
		tagStateKey int
		tagRootKey  int
		snapSeqKey  int
		aggIDArg    int
		statusArg   int
		statusAtArg int
		tagCountArg int
		firstTagArg int
		nextArg     int
	}

	luaAppendCall struct {
		keys []string
		args []any
		spec luaAppendSpec
	}

	luaAppendInput struct {
		statusAt time.Time
		status   *string
		tags     map[string]bool
		id       timebox.AggregateID
		events   [][]byte
		atSeq    int64
	}

	luaAppendOp struct {
		tag string
		add bool
	}

	luaAppendSpec struct {
		trim   bool
		status bool
		tags   bool
	}
)

const (
	appendChunkedLua = `
		local chunkSize = 128
		local eventCount = tonumber(ARGV[2])
		local startIdx = eventStartIdx
		local lastEventIdx = eventStartIdx + eventCount - 1

		while startIdx <= lastEventIdx do
			local endIdx = math.min(startIdx + chunkSize - 1, lastEventIdx)
			local chunk = {}
			for i = startIdx, endIdx do
				table.insert(chunk, ARGV[i])
			end
			redis.call('RPUSH', KEYS[1], unpack(chunk))
			startIdx = endIdx + 1
		end
		`

	appendProjectStatusLua = `
		local statusSetPrefix = KEYS[2] .. ":"
		local oldStatus = redis.call('HGET', KEYS[2], aggID) or ""
		if oldStatus ~= "" and oldStatus ~= newStatus then
			redis.call('ZREM', statusSetPrefix .. oldStatus, aggID)
		end
		if newStatus ~= "" then
			redis.call('HSET', KEYS[2], aggID, newStatus)
			if oldStatus ~= newStatus then
				redis.call(
					'ZADD', statusSetPrefix .. newStatus, newStatusAt, aggID
				)
			end
		else
			redis.call('HDEL', KEYS[2], aggID)
		end
		`

	appendProjectTagsLua = `
		for i = 0, tagCount - 1 do
			local argIdx = firstTagArgIdx + (i * 2)
			local tag = ARGV[argIdx]
			local add = ARGV[argIdx + 1] == "1"
			local memberKey = KEYS[tagRootKeyIdx] .. ":" .. tag
			if add then
				redis.call('SADD', KEYS[tagStateKeyIdx], tag)
				redis.call('SADD', memberKey, aggID)
			else
				redis.call('SREM', KEYS[tagStateKeyIdx], tag)
				redis.call('SREM', memberKey, aggID)
			end
		end
		`

	appendSequenceCheckLua = `
		if expected ~= currentSeq then
			if expected < currentSeq then
				local startIndex = expected - offset
				if startIndex < 0 then
					return {0, currentSeq, {}}
				end
				local newEvents = redis.call('LRANGE', KEYS[1], startIndex, -1)
				return {0, currentSeq, newEvents}
			end
			return {0, currentSeq, {}}
		end
		`
)

func makeLuaAppendScripts() map[luaAppendSpec]*redis.Script {
	res := map[luaAppendSpec]*redis.Script{}
	for _, trim := range []bool{false, true} {
		for _, status := range []bool{false, true} {
			for _, tags := range []bool{false, true} {
				spec := luaAppendSpec{
					trim:   trim,
					status: status,
					tags:   tags,
				}
				res[spec] = redis.NewScript(buildAppendLua(spec))
			}
		}
	}
	return res
}

func newLuaAppendBuilder(spec luaAppendSpec) *luaAppendBuilder {
	b := &luaAppendBuilder{
		spec:    spec,
		nextArg: 3,
	}

	b.initKeyLayout()
	b.initArgLayout()
	return b
}

func (b *luaAppendBuilder) writePreamble() {
	b.write(
		`-- Atomically append events to list with sequence consistency check`,
		`local currentLen = redis.call('LLEN', KEYS[1])`,
		`local expected = tonumber(ARGV[1])`,
	)
	if b.spec.trim {
		b.writef(
			`local offset = tonumber(redis.call('GET', KEYS[%d]) or "0")`,
			b.snapSeqKey,
		)
		b.write(`local currentSeq = offset + currentLen`)
		return
	}
	b.write(
		`local offset = 0`,
		`local currentSeq = currentLen`,
	)
}

func (b *luaAppendBuilder) writeLocals() {
	if b.spec.status || b.spec.tags {
		b.writef(`local aggID = ARGV[%d]`, b.aggIDArg)
	}
	if b.spec.status {
		b.writef(`local newStatus = ARGV[%d]`, b.statusArg)
		b.writef(`local newStatusAt = ARGV[%d]`, b.statusAtArg)
	}
	if b.spec.tags {
		b.writef(`local tagStateKeyIdx = %d`, b.tagStateKey)
		b.writef(`local tagRootKeyIdx = %d`, b.tagRootKey)
		b.writef(`local tagCount = tonumber(ARGV[%d]) or 0`,
			b.tagCountArg,
		)
		b.writef(`local firstTagArgIdx = %d`, b.firstTagArg)
	}
	b.writef(`local eventStartIdx = %s`, b.eventStartExpr())
}

func (b *luaAppendBuilder) eventStartExpr() string {
	if !b.spec.tags {
		return fmt.Sprintf(`%d`, b.nextArg)
	}
	return fmt.Sprintf(
		`%d + (tonumber(ARGV[%d]) * 2)`,
		b.firstTagArg,
		b.tagCountArg,
	)
}

func (b *luaAppendBuilder) writeBody() {
	b.write(appendSequenceCheckLua)
	b.write(appendChunkedLua)
	if b.spec.status {
		b.write(appendProjectStatusLua)
	}
	if b.spec.tags {
		b.write(appendProjectTagsLua)
	}
	b.write(`return {1, offset + redis.call('LLEN', KEYS[1])}`)
}

func (b *luaAppendBuilder) initKeyLayout() {
	keyIdx := 2
	if b.spec.status {
		keyIdx++
	}
	if b.spec.tags {
		b.tagStateKey = keyIdx
		b.tagRootKey = keyIdx + 1
		keyIdx += 2
	}
	if b.spec.trim {
		b.snapSeqKey = keyIdx
	}
}

func (b *luaAppendBuilder) initArgLayout() {
	if b.spec.status || b.spec.tags {
		b.aggIDArg = b.nextArg
		b.nextArg++
	}
	if b.spec.status {
		b.statusArg = b.nextArg
		b.statusAtArg = b.nextArg + 1
		b.nextArg += 2
	}
	if b.spec.tags {
		b.tagCountArg = b.nextArg
		b.firstTagArg = b.nextArg + 1
		b.nextArg++
	}
}

func (b *luaAppendBuilder) write(lines ...string) {
	for _, line := range lines {
		_, _ = fmt.Fprintf(&b.out, "%s\n", line)
	}
}

func (b *luaAppendBuilder) writef(f string, args ...any) {
	_, _ = fmt.Fprintf(&b.out, f, args...)
	_, _ = fmt.Fprint(&b.out, "\n")
}

func buildAppendLua(spec luaAppendSpec) string {
	b := newLuaAppendBuilder(spec)
	b.writePreamble()
	b.writeLocals()
	b.writeBody()
	return b.out.String()
}

func buildLuaAppendCall(
	store *timebox.Store, p *Persistence, in luaAppendInput,
) luaAppendCall {
	ops := newLuaAppendOps(in.tags)
	spec := luaAppendSpec{
		trim:   store.Config().TrimEvents,
		status: in.status != nil,
		tags:   len(ops) > 0,
	}
	return luaAppendCall{
		spec: spec,
		keys: buildLuaAppendKeys(p, in.id, spec),
		args: buildLuaAppendArgs(joinAggregateID(in.id), in, ops, spec),
	}
}

func buildLuaAppendKeys(
	p *Persistence, id timebox.AggregateID, spec luaAppendSpec,
) []string {
	keys := []string{p.buildKey(id, eventsSuffix)}
	if spec.status {
		keys = append(keys, p.buildStatusHashKey())
	}
	if spec.tags {
		keys = append(keys, p.buildTagStateKey(id))
		keys = append(keys, p.buildTagRootKey())
	}
	if spec.trim {
		keys = append(keys, p.buildKey(id, snapshotSeqSuffix))
	}
	return keys
}

func buildLuaAppendArgs(
	joinedID string, in luaAppendInput, ops []luaAppendOp, spec luaAppendSpec,
) []any {
	args := []any{in.atSeq, len(in.events)}
	if spec.status || spec.tags {
		args = append(args, joinedID)
	}
	if spec.status {
		status := ""
		if in.status != nil {
			status = *in.status
		}
		args = append(args, status, in.statusAt.UnixMilli())
	}
	if spec.tags {
		args = append(args, len(ops))
		for _, op := range ops {
			args = append(args, op.tag, op.add)
		}
	}
	for _, ev := range in.events {
		args = append(args, ev)
	}
	return args
}

func newLuaAppendOps(tags map[string]bool) []luaAppendOp {
	ops := make([]luaAppendOp, 0, len(tags))
	for tag, add := range tags {
		ops = append(ops, luaAppendOp{
			tag: escapeKeyPart(tag),
			add: add,
		})
	}
	return ops
}

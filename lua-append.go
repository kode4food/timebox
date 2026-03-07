package timebox

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type (
	luaAppendBuilder struct {
		out           strings.Builder
		spec          luaAppendSpec
		labelStateKey int
		labelRootKey  int
		snapSeqKey    int
		aggIDArg      int
		statusArg     int
		statusAtArg   int
		labelCountArg int
		firstLabelArg int
		nextArg       int
	}

	luaAppendCall struct {
		spec luaAppendSpec
		keys []string
		args []any
	}

	luaAppendInput struct {
		id       AggregateID
		atSeq    int64
		status   *string
		statusAt string
		labels   map[string]string
		events   []string
	}

	luaAppendOp struct {
		label string
		op    string
		value string
	}

	luaAppendSpec struct {
		trim   bool
		status bool
		labels bool
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

	appendProjectLabelsLua = `
		for i = 0, labelCount - 1 do
			local argIdx = firstLabelArgIdx + (i * 3)
			local label = ARGV[argIdx]
			local op = ARGV[argIdx + 1]
			local value = ARGV[argIdx + 2]
			local oldValue = redis.call(
				'HGET', KEYS[labelStateKeyIdx], label
			) or ""
			local valueKey = KEYS[labelRootKeyIdx] .. ":" .. label
			if op == "set" and oldValue == value then
				goto continue
			end
			if oldValue ~= "" then
				local oldMemberKey = KEYS[labelRootKeyIdx]
					.. ":" .. label .. ":" .. oldValue
				redis.call('SREM', oldMemberKey, aggID)
				if redis.call('SCARD', oldMemberKey) == 0 then
					redis.call('SREM', valueKey, oldValue)
				end
			end
			if op == "set" then
				local newMemberKey = KEYS[labelRootKeyIdx]
					.. ":" .. label .. ":" .. value
				redis.call('SADD', valueKey, value)
				redis.call('SADD', newMemberKey, aggID)
				redis.call('HSET', KEYS[labelStateKeyIdx], label, value)
			else
				redis.call('HDEL', KEYS[labelStateKeyIdx], label)
			end
			::continue::
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
			for _, labels := range []bool{false, true} {
				spec := luaAppendSpec{
					trim:   trim,
					status: status,
					labels: labels,
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
	if b.spec.status || b.spec.labels {
		b.writef(`local aggID = ARGV[%d]`, b.aggIDArg)
	}
	if b.spec.status {
		b.writef(`local newStatus = ARGV[%d]`, b.statusArg)
		b.writef(`local newStatusAt = ARGV[%d]`, b.statusAtArg)
	}
	if b.spec.labels {
		b.writef(`local labelStateKeyIdx = %d`, b.labelStateKey)
		b.writef(`local labelRootKeyIdx = %d`, b.labelRootKey)
		b.writef(`local labelCount = tonumber(ARGV[%d]) or 0`,
			b.labelCountArg,
		)
		b.writef(`local firstLabelArgIdx = %d`, b.firstLabelArg)
	}
	b.writef(`local eventStartIdx = %s`, b.eventStartExpr())
}

func (b *luaAppendBuilder) eventStartExpr() string {
	if !b.spec.labels {
		return fmt.Sprintf(`%d`, b.nextArg)
	}
	return fmt.Sprintf(
		`%d + (tonumber(ARGV[%d]) * 3)`,
		b.firstLabelArg,
		b.labelCountArg,
	)
}

func (b *luaAppendBuilder) writeBody() {
	b.write(appendSequenceCheckLua)
	b.write(appendChunkedLua)
	if b.spec.status {
		b.write(appendProjectStatusLua)
	}
	if b.spec.labels {
		b.write(appendProjectLabelsLua)
	}
	b.write(`return {1, offset + redis.call('LLEN', KEYS[1])}`)
}

func (b *luaAppendBuilder) initKeyLayout() {
	keyIdx := 2
	if b.spec.status {
		keyIdx++
	}
	if b.spec.labels {
		b.labelStateKey = keyIdx
		b.labelRootKey = keyIdx + 1
		keyIdx += 2
	}
	if b.spec.trim {
		b.snapSeqKey = keyIdx
	}
}

func (b *luaAppendBuilder) initArgLayout() {
	if b.spec.status || b.spec.labels {
		b.aggIDArg = b.nextArg
		b.nextArg++
	}
	if b.spec.status {
		b.statusArg = b.nextArg
		b.statusAtArg = b.nextArg + 1
		b.nextArg += 2
	}
	if b.spec.labels {
		b.labelCountArg = b.nextArg
		b.firstLabelArg = b.nextArg + 1
		b.nextArg++
	}
}

func (b *luaAppendBuilder) write(lines ...string) {
	for _, line := range lines {
		fmt.Fprintf(&b.out, "%s\n", line)
	}
}

func (b *luaAppendBuilder) writef(f string, args ...any) {
	fmt.Fprintf(&b.out, f, args...)
	fmt.Fprint(&b.out, "\n")
}

func buildAppendLua(spec luaAppendSpec) string {
	b := newLuaAppendBuilder(spec)
	b.writePreamble()
	b.writeLocals()
	b.writeBody()
	return b.out.String()
}

func buildLuaAppendCall(s *Store, in luaAppendInput) luaAppendCall {
	ops := newLuaAppendOps(in.labels)
	spec := luaAppendSpec{
		trim:   s.config.TrimEvents,
		status: in.status != nil,
		labels: len(ops) > 0,
	}
	return luaAppendCall{
		spec: spec,
		keys: buildLuaAppendKeys(s, in.id, spec),
		args: buildLuaAppendArgs(in, ops, spec),
	}
}

func buildLuaAppendKeys(
	s *Store, id AggregateID, spec luaAppendSpec,
) []string {
	keys := []string{s.buildKey(id, eventsSuffix)}
	if spec.status {
		keys = append(keys, s.buildStatusHashKey())
	}
	if spec.labels {
		keys = append(keys, s.buildLabelStateKey(id))
		keys = append(keys, s.buildLabelRootKey())
	}
	if spec.trim {
		keys = append(keys, s.buildKey(id, snapshotSeqSuffix))
	}
	return keys
}

func buildLuaAppendArgs(
	in luaAppendInput, ops []luaAppendOp, spec luaAppendSpec,
) []any {
	args := []any{in.atSeq, len(in.events)}
	if spec.status || spec.labels {
		args = append(args, in.id.Join(":"))
	}
	if spec.status {
		status := ""
		if in.status != nil {
			status = *in.status
		}
		args = append(args, status, in.statusAt)
	}
	if spec.labels {
		args = append(args, len(ops))
		for _, op := range ops {
			args = append(args, op.label, op.op, op.value)
		}
	}
	for _, ev := range in.events {
		args = append(args, ev)
	}
	return args
}

func newLuaAppendOps(lbls map[string]string) []luaAppendOp {
	ops := make([]luaAppendOp, 0, len(lbls))
	for label, value := range lbls {
		op := "set"
		if value == "" {
			op = "remove"
		}
		ops = append(ops, luaAppendOp{
			op:    op,
			label: escapeKeyPart(label),
			value: escapeKeyPart(value),
		})
	}
	return ops
}

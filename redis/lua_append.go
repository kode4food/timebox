package redis

import (
	"time"

	"github.com/kode4food/timebox"
)

type (
	luaAppendInput struct {
		statusAt time.Time
		status   *string
		tags     map[string]bool
		id       timebox.AggregateID
		events   [][]byte
		atSeq    int64
	}

	// luaAppendSpec records which projections a request needs, and so which
	// of its keys and args the script should expect
	luaAppendSpec struct {
		trim   bool
		status bool
		tags   bool
	}

	luaAppendOp struct {
		tag string
		add bool
	}
)

const (
	// flag bits telling the script which optional keys and args a request
	// carries. Lua 5.1 has no bitwise operators, so the script divides
	luaAppendStatus = 1
	luaAppendTags   = 2
	luaAppendTrim   = 4
)

// appendLuaCall adds one request's keys and args in the order the script's
// cursors claim them
func (p *Persistence) appendLuaCall(
	keys []string, args []any, in luaAppendInput,
) ([]string, []any) {
	ops := newLuaAppendOps(in.tags)
	spec := luaAppendSpec{
		trim:   p.cfg.Timebox.TrimEvents,
		status: in.status != nil,
		tags:   len(ops) > 0,
	}
	keys = append(keys, p.buildLuaAppendKeys(in.id, spec)...)
	args = append(args, buildLuaAppendArgs(
		joinAggregateID(in.id), in, ops, spec,
	)...)
	return keys, args
}

// buildLuaAppendKeys lists a request's keys in the order the script's key
// cursor claims them
func (p *Persistence) buildLuaAppendKeys(
	id timebox.AggregateID, spec luaAppendSpec,
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

// buildLuaAppendArgs lists a request's args in the order the script's arg
// cursor reads them, led by the flags naming its optional parts
func buildLuaAppendArgs(
	joinedID string, in luaAppendInput, ops []luaAppendOp, spec luaAppendSpec,
) []any {
	flags := 0
	if spec.status {
		flags |= luaAppendStatus
	}
	if spec.tags {
		flags |= luaAppendTags
	}
	if spec.trim {
		flags |= luaAppendTrim
	}

	args := []any{flags, in.atSeq, len(in.events), joinedID}
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

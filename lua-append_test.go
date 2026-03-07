package timebox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLuaAppendBuilder(t *testing.T) {
	for _, tt := range []struct {
		name             string
		spec             luaAppendSpec
		labelStateKeyIdx int
		labelRootKeyIdx  int
		snapSeqKeyIdx    int
		aggIDArgIdx      int
		statusArgIdx     int
		statusAtArgIdx   int
		labelCountArgIdx int
		firstLabelArgIdx int
		nextArgIdx       int
		eventStartExpr   string
	}{
		{
			name:           "plain",
			spec:           luaAppendSpec{},
			nextArgIdx:     3,
			eventStartExpr: "3",
		},
		{
			name:           "plain-trimmed",
			spec:           luaAppendSpec{trim: true},
			snapSeqKeyIdx:  2,
			nextArgIdx:     3,
			eventStartExpr: "3",
		},
		{
			name:           "status",
			spec:           luaAppendSpec{status: true},
			aggIDArgIdx:    3,
			statusArgIdx:   4,
			statusAtArgIdx: 5,
			nextArgIdx:     6,
			eventStartExpr: "6",
		},
		{
			name:             "labels",
			spec:             luaAppendSpec{labels: true},
			labelStateKeyIdx: 2,
			labelRootKeyIdx:  3,
			aggIDArgIdx:      3,
			labelCountArgIdx: 4,
			firstLabelArgIdx: 5,
			nextArgIdx:       5,
			eventStartExpr:   "5 + (tonumber(ARGV[4]) * 3)",
		},
		{
			name: "trimmed-status-labels",
			spec: luaAppendSpec{
				trim: true, status: true, labels: true,
			},
			labelStateKeyIdx: 3,
			labelRootKeyIdx:  4,
			snapSeqKeyIdx:    5,
			aggIDArgIdx:      3,
			statusArgIdx:     4,
			statusAtArgIdx:   5,
			labelCountArgIdx: 6,
			firstLabelArgIdx: 7,
			nextArgIdx:       7,
			eventStartExpr:   "7 + (tonumber(ARGV[6]) * 3)",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := newLuaAppendBuilder(tt.spec)

			assert.Equal(t, tt.labelStateKeyIdx, b.labelStateKey)
			assert.Equal(t, tt.labelRootKeyIdx, b.labelRootKey)
			assert.Equal(t, tt.snapSeqKeyIdx, b.snapSeqKey)
			assert.Equal(t, tt.aggIDArgIdx, b.aggIDArg)
			assert.Equal(t, tt.statusArgIdx, b.statusArg)
			assert.Equal(t, tt.statusAtArgIdx, b.statusAtArg)
			assert.Equal(t, tt.labelCountArgIdx, b.labelCountArg)
			assert.Equal(t, tt.firstLabelArgIdx, b.firstLabelArg)
			assert.Equal(t, tt.nextArgIdx, b.nextArg)
			assert.Equal(t, tt.eventStartExpr, b.eventStartExpr())
		})
	}
}

func TestBuildLuaAppendCall(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		s := &Store{
			prefix: "tb",
			config: StoreConfig{JoinKey: JoinKey},
		}
		id := NewAggregateID("order", "1")

		call := buildLuaAppendCall(s, luaAppendInput{
			id:     id,
			atSeq:  3,
			labels: map[string]string{},
			events: []string{"ev-1", "ev-2"},
		})

		assert.Equal(t, luaAppendSpec{}, call.spec)
		assert.Equal(t, []string{"tb:order:1:events"}, call.keys)
		assert.Equal(t, []any{int64(3), 2, "ev-1", "ev-2"}, call.args)
	})

	t.Run("trimmed-status-labels", func(t *testing.T) {
		s := &Store{
			prefix: "tb",
			config: StoreConfig{
				JoinKey:    JoinKey,
				TrimEvents: true,
			},
		}
		id := NewAggregateID("order", "1")
		status := "active"

		call := buildLuaAppendCall(s, luaAppendInput{
			id:       id,
			atSeq:    7,
			status:   &status,
			statusAt: "1234",
			labels: map[string]string{
				"env":    "prod",
				"region": "",
			},
			events: []string{"ev-1"},
		})

		assert.Equal(t, luaAppendSpec{
			trim:   true,
			status: true,
			labels: true,
		}, call.spec)
		assert.Equal(t, []string{
			"tb:order:1:events",
			"tb:idx:status",
			"tb:idx:labels:order:1",
			"tb:idx:label",
			"tb:order:1:snapshot:seq",
		}, call.keys)
		assert.Equal(t, []any{
			int64(7),
			1,
			"order:1",
			"active",
			"1234",
			2,
			"env",
			"set",
			"prod",
			"region",
			"remove",
			"",
			"ev-1",
		}, call.args)
	})
}

func TestNewLuaAppendScripts(t *testing.T) {
	scripts := makeLuaAppendScripts()

	assert.Len(t, scripts, 8)
	for _, trim := range []bool{false, true} {
		for _, status := range []bool{false, true} {
			for _, labels := range []bool{false, true} {
				spec := luaAppendSpec{
					trim:   trim,
					status: status,
					labels: labels,
				}
				assert.NotNil(t, scripts[spec])
			}
		}
	}
}

func TestBuildAppendLua(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		lua := buildAppendLua(luaAppendSpec{})

		assert.Contains(t, lua, "local offset = 0")
		assert.Contains(t, lua, "local eventStartIdx = 3")
		assert.NotContains(t, lua, "local aggID =")
		assert.NotContains(t, lua, "local newStatus =")
		assert.NotContains(t, lua, "local labelCount =")
	})

	t.Run("status-only", func(t *testing.T) {
		lua := buildAppendLua(luaAppendSpec{status: true})

		assert.Contains(t, lua, "local aggID = ARGV[3]")
		assert.Contains(t, lua, "local newStatus = ARGV[4]")
		assert.Contains(t, lua, "local newStatusAt = ARGV[5]")
		assert.Contains(t, lua, "local eventStartIdx = 6")
		assert.Contains(t, lua, `local statusSetPrefix = KEYS[2] .. ":"`)
		assert.NotContains(t, lua, "local labelCount =")
	})

	t.Run("labels-only", func(t *testing.T) {
		lua := buildAppendLua(luaAppendSpec{labels: true})

		assert.Contains(t, lua, "local aggID = ARGV[3]")
		assert.Contains(t, lua, "local labelStateKeyIdx = 2")
		assert.Contains(t, lua, "local labelRootKeyIdx = 3")
		assert.Contains(t, lua, "local labelCount = tonumber(ARGV[4]) or 0")
		assert.Contains(t, lua, "local firstLabelArgIdx = 5")
		assert.Contains(t, lua,
			"local eventStartIdx = 5 + (tonumber(ARGV[4]) * 3)",
		)
		assert.NotContains(t, lua, "local newStatus =")
		assert.NotContains(t, lua, "local statusSetPrefix =")
	})

	t.Run("trimmed-status-labels", func(t *testing.T) {
		lua := buildAppendLua(luaAppendSpec{
			trim:   true,
			status: true,
			labels: true,
		})

		assert.Contains(t, lua,
			`local offset = tonumber(redis.call('GET', KEYS[5]) or "0")`,
		)
		assert.Contains(t, lua, "local aggID = ARGV[3]")
		assert.Contains(t, lua, "local newStatus = ARGV[4]")
		assert.Contains(t, lua, "local newStatusAt = ARGV[5]")
		assert.Contains(t, lua, "local labelStateKeyIdx = 3")
		assert.Contains(t, lua, "local labelRootKeyIdx = 4")
		assert.Contains(t, lua, "local labelCount = tonumber(ARGV[6]) or 0")
		assert.Contains(t, lua, "local firstLabelArgIdx = 7")
		assert.Contains(t, lua,
			"local eventStartIdx = 7 + (tonumber(ARGV[6]) * 3)",
		)
	})
}

package timebox_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestAggregateID(t *testing.T) {
	id := timebox.NewAggregateID("counter", "123")
	assert.Len(t, id, 2)

	joined := id.Join(":")
	assert.Equal(t, "counter:123", joined)

	parsed := timebox.ParseAggregateID("counter:123", ":")
	assert.Equal(t, id, parsed)
}

func TestJoinKeySlotted(t *testing.T) {
	fn1 := timebox.JoinKeySlotted(1)
	assert.Equal(t, "{flow}:abc", fn1(timebox.NewAggregateID("flow", "abc")))
	assert.Equal(t,
		"{flow}:abc:xyz", fn1(timebox.NewAggregateID("flow", "abc", "xyz")),
	)

	fn2 := timebox.JoinKeySlotted(2)
	assert.Equal(t,
		"{flow:abc}:xyz", fn2(timebox.NewAggregateID("flow", "abc", "xyz")),
	)

	// n >= len(id) clamps to full ID in slot
	fnBig := timebox.JoinKeySlotted(99)
	assert.Equal(t, "{flow:abc}", fnBig(timebox.NewAggregateID("flow", "abc")))
}

func TestParseKeySlotted(t *testing.T) {
	fn := timebox.ParseKeySlotted(1)
	assert.Equal(t,
		timebox.NewAggregateID("flow", "abc"),
		fn("{flow}:abc"),
	)
	assert.Equal(t,
		timebox.NewAggregateID("flow", "abc", "xyz"),
		fn("{flow}:abc:xyz"),
	)
	assert.Equal(t,
		timebox.NewAggregateID("flow", "abc"),
		fn("{flow:abc}"),
	)
}

func TestAggregateIDEqual(t *testing.T) {
	id := timebox.NewAggregateID("order", "1")
	same := timebox.NewAggregateID("order", "1")
	diffValue := timebox.NewAggregateID("order", "2")
	diffLen := timebox.NewAggregateID("order", "1", "extra")

	assert.True(t, id.Equal(same))
	assert.False(t, id.Equal(diffValue))
	assert.False(t, id.Equal(diffLen))
}

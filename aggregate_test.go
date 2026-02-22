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

func TestSlotByLeadingParts(t *testing.T) {
	fn1 := timebox.SlotByLeadingParts(1)
	assert.Equal(t, "flow", fn1(timebox.NewAggregateID("flow", "abc")))
	assert.Equal(t, "flow", fn1(timebox.NewAggregateID("flow", "abc", "xyz")))

	fn2 := timebox.SlotByLeadingParts(2)
	assert.Equal(t,
		"flow:abc", fn2(timebox.NewAggregateID("flow", "abc", "xyz")),
	)

	// n >= len(id) clamps to full ID
	fnBig := timebox.SlotByLeadingParts(99)
	assert.Equal(t, "flow:abc", fnBig(timebox.NewAggregateID("flow", "abc")))
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

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

func TestAggregateIDEqual(t *testing.T) {
	id := timebox.NewAggregateID("order", "1")
	same := timebox.NewAggregateID("order", "1")
	diffValue := timebox.NewAggregateID("order", "2")
	diffLen := timebox.NewAggregateID("order", "1", "extra")

	assert.True(t, id.Equal(same))
	assert.False(t, id.Equal(diffValue))
	assert.False(t, id.Equal(diffLen))
}

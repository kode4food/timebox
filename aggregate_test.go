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

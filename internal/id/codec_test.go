package id_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/id"
)

func TestCodec(t *testing.T) {
	join, parse := id.MakeCodec(':')

	first := timebox.NewAggregateID(`order:1`, `path\part`, `%done`)
	second := timebox.NewAggregateID("order", "1", `path\part`)

	assert.Equal(t, first, parse(join(first)))
	assert.Equal(t, second, parse(join(second)))
	assert.NotEqual(t, join(first), join(second))
}

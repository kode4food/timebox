package id_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/internal/id"
)

func TestCodec(t *testing.T) {
	join, parse := id.MakeCodec(':')

	tests := []struct {
		name  string
		aggID timebox.AggregateID
	}{
		{
			name:  "escapes separator and slash",
			aggID: timebox.NewAggregateID(`order:1`, `path\\part:%done`),
		},
		{
			name:  "simple segments",
			aggID: timebox.NewAggregateID("order", "1"),
		},
		{
			name:  "type only",
			aggID: timebox.NewAggregateType("order"),
		},
		{
			name:  "empty parts",
			aggID: timebox.AggregateID{},
		},
		{
			name:  "trailing slash",
			aggID: timebox.NewAggregateType(`path\\`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.aggID, parse(join(tc.aggID)))
		})
	}

	left := timebox.NewAggregateID(`order:1`, `path\\part`)
	right := timebox.NewAggregateID("order", `1:path\\part`)
	assert.NotEqual(t, join(left), join(right))
}

func TestParts(t *testing.T) {
	assert.Equal(t,
		[]string{"order", "1"},
		id.Parts[string](timebox.NewAggregateID("order", "1")),
	)
	assert.Equal(t,
		[]string{"order"},
		id.Parts[string](timebox.NewAggregateType("order")),
	)
	assert.Empty(t, id.Parts[string](timebox.AggregateID{}))
}

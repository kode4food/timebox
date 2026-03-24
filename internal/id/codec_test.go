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
			aggID: timebox.NewAggregateID(`order:1`, `path\\part`, `%done`),
		},
		{
			name:  "simple segments",
			aggID: timebox.NewAggregateID("order", "1", `path\\part`),
		},
		{
			name:  "empty parts",
			aggID: timebox.NewAggregateID("", ""),
		},
		{
			name:  "trailing slash",
			aggID: timebox.NewAggregateID(`path\\`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.aggID, parse(join(tc.aggID)))
		})
	}

	left := timebox.NewAggregateID(`order:1`, `path\\part`, `%done`)
	right := timebox.NewAggregateID("order", "1", `path\\part`)
	assert.NotEqual(t, join(left), join(right))
}

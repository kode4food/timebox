package timebox_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestAggregateID(t *testing.T) {
	id := timebox.NewAggregateID("counter", "123")
	assert.Equal(t, timebox.ID("counter"), id.Type)
	assert.Equal(t, timebox.ID("123"), id.Key)
	assert.Equal(t, `["counter","123"]`, id.String())
}

func TestAggregateType(t *testing.T) {
	id := timebox.NewAggregateType("counter")
	assert.Equal(t, timebox.ID("counter"), id.Type)
	assert.Equal(t, timebox.SingletonKey, id.Key)
	assert.Equal(t, `["counter","_"]`, id.String())
}

func TestAggregateIDStringEscapes(t *testing.T) {
	id := timebox.NewAggregateID(`order:1`, `"part"\part`)
	assert.Equal(t, `["order:1","\"part\"\\part"]`, id.String())
}

func TestAggregateIDComparable(t *testing.T) {
	id := timebox.NewAggregateID("order", "1")
	same := timebox.NewAggregateID("order", "1")
	diffKey := timebox.NewAggregateID("order", "2")
	diffType := timebox.NewAggregateID("invoice", "1")

	assert.True(t, id == same)
	assert.False(t, id == diffKey)
	assert.False(t, id == diffType)

	counts := map[timebox.AggregateID]int{id: 1}
	counts[same]++
	assert.Equal(t, 2, counts[id])
	assert.Len(t, counts, 1)
}

func TestAggregateIDJSON(t *testing.T) {
	for _, id := range []timebox.AggregateID{
		timebox.NewAggregateID("order", "1"),
		timebox.NewAggregateType("order"),
		{},
	} {
		data, err := json.Marshal(id)
		assert.NoError(t, err)

		var res timebox.AggregateID
		assert.NoError(t, json.Unmarshal(data, &res))
		assert.Equal(t, id, res)
	}

	data, err := json.Marshal(timebox.NewAggregateID("order", "1"))
	assert.NoError(t, err)
	assert.Equal(t, `["order","1"]`, string(data))

	var res timebox.AggregateID
	err = json.Unmarshal([]byte(`["order","1","item"]`), &res)
	assert.ErrorIs(t, err, timebox.ErrInvalidAggregateID)
	err = json.Unmarshal([]byte(`["order"]`), &res)
	assert.ErrorIs(t, err, timebox.ErrInvalidAggregateID)
	assert.Error(t, json.Unmarshal([]byte(`"order"`), &res))
}

package timebox_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
)

func TestPublishAppends(t *testing.T) {
	var got []*timebox.Event
	calls := 0
	p := timebox.Publisher(func(evs ...*timebox.Event) {
		calls++
		got = evs
	})
	first := &timebox.Event{Type: "first"}
	second := &timebox.Event{Type: "second"}

	p.PublishAppends(
		timebox.AppendRequest{Events: []*timebox.Event{first}},
		timebox.AppendRequest{Events: []*timebox.Event{second}},
	)
	assert.Equal(t, 1, calls)
	assert.Equal(t, []*timebox.Event{first, second}, got)

	p.PublishAppends(timebox.AppendRequest{})
	assert.Equal(t, 1, calls)

	timebox.Publisher(nil).PublishAppends(timebox.AppendRequest{
		Events: []*timebox.Event{first},
	})
}

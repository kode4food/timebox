package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

func TestAppendBadStatusAt(t *testing.T) {
	withTestDatabase(t, func(_ context.Context, cfg postgres.Config) {
		p, err := postgres.NewPersistence(cfg)
		if !assert.NoError(t, err) {
			return
		}
		defer func() { _ = p.Close() }()

		s := "active"
		_, err = p.Append(timebox.AppendRequest{
			ID:               timebox.NewAggregateID("o", "1"),
			ExpectedSequence: 0,
			Status:           &s,
			StatusAt:         "not-a-number",
			Events: []*timebox.Event{
				testEvent(t, time.Unix(0, 0).UTC(), "", "", 0),
			},
		})
		assert.Error(t, err)
	})
}

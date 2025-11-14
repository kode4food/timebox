package timebox_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kode4food/timebox"
)

func TestVersionConflictError(t *testing.T) {
	err := &timebox.VersionConflictError{
		ExpectedSequence: 0,
		ActualSequence:   5,
		NewEvents:        []*timebox.Event{{}, {}},
	}

	assert.Contains(t, err.Error(), "version conflict")
	assert.Contains(t, err.Error(), "expected sequence 0")
	assert.Contains(t, err.Error(), "but at 5")
}

func TestStoreOperations(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	cfg := timebox.DefaultConfig()
	storeCfg := cfg.Store
	storeCfg.Addr = server.Addr()

	tb, err := timebox.NewTimebox(cfg)
	require.NoError(t, err)
	defer func() { _ = tb.Close() }()

	store, err := tb.NewStore(storeCfg)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	id := timebox.NewAggregateID("test", "1")

	ev := &timebox.Event{
		Type:        EventIncremented,
		AggregateID: id,
		Timestamp:   time.Now(),
		Data:        json.RawMessage(`5`),
	}

	err = store.AppendEvents(ctx, id, 0, []*timebox.Event{ev})
	require.NoError(t, err)

	events, err := store.GetEvents(ctx, id, 0)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, EventIncremented, events[0].Type)
}

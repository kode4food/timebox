package dbos_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	tbdbos "github.com/kode4food/timebox/dbos"
)

type workArgs struct {
	queue     timebox.AggregateID
	old       timebox.AggregateID
	scheduled timebox.AggregateID
	at        time.Time
}

var errRetry = errors.New("retry delivery")

func TestDelivery(t *testing.T) {
	cfg := testConfig(t)
	cfg.Namespace = "native"
	b := openBackend(t, cfg)
	s := newStore(t, b)

	client := openClient(t, cfg)

	queued := timebox.NewAggregateID("job", "queued")
	old := timebox.NewAggregateID("job", "old")
	scheduled := timebox.NewAggregateID("job", "scheduled")
	at := time.Now().Add(1250 * time.Millisecond)
	publishWork(t, s, workArgs{
		queue:     queued,
		old:       old,
		scheduled: scheduled,
		at:        at,
	})

	for kind, count := range map[dbos.WorkflowStatusType]int{
		dbos.WorkflowStatusEnqueued: 1,
		dbos.WorkflowStatusDelayed:  2,
	} {
		workflows, err := dbos.ListWorkflows(client, dbos.WithFilterName("timebox-deliver"),
			dbos.WithFilterStatus(kind))
		assert.NoError(t, err)
		assert.Len(t, workflows, count)
	}

	err := s.Transact(func(tx *timebox.Transaction) error {
		return tx.Publish("drained", &timebox.Message{
			Type:        "queue",
			AggregateID: queued,
		})
	})
	assert.NoError(t, err)
	err = s.Consume(t.Context(), "drained", func(_ *timebox.Transaction, _ *timebox.Message) error {
		return nil
	})
	assert.NoError(t, err)

	// Workers start from a fresh runtime and process the notifications already in the database.
	if !assert.NoError(t, b.Close()) {
		t.FailNow()
	}
	b = openBackend(t, cfg)
	s = newStore(t, b)
	exec := s.Executor(func() timebox.Empty { return timebox.Empty{} },
		func(st timebox.Empty, _ []*timebox.Event) timebox.Empty { return st },
	)
	var queueCalls atomic.Int32
	handled := func(tx *timebox.Transaction, msg *timebox.Message) error {
		_, err := tx.Exec(exec, msg.AggregateID,
			func(_ timebox.Empty, ag *timebox.Aggregator[timebox.Empty]) error {
				return ag.Raise("handled", timebox.Empty{})
			},
		)
		return err
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- b.RunDeliveries(ctx, s, tbdbos.DeliveryHandlers{
			Queue: func(tx *timebox.Transaction, key timebox.ID, msg *timebox.Message) error {
				assert.Equal(t, timebox.ID("jobs"), key)
				if err := handled(tx, msg); err != nil {
					return err
				}
				if queueCalls.Add(1) == 1 {
					return errRetry
				}
				return nil
			},
			Schedule: func(tx *timebox.Transaction, msg *timebox.Message) error {
				assert.False(t, time.Now().Before(at))
				return handled(tx, msg)
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(15 * time.Second):
			assert.Fail(t, "timed out waiting for the delivery worker")
		}
	})

	assert.Eventually(t, func() bool {
		workflows, err := dbos.ListWorkflows(client,
			dbos.WithFilterName("timebox-deliver"), dbos.WithFilterStatus(dbos.WorkflowStatusSuccess))
		return err == nil && len(workflows) == 4
	}, 10*time.Second, 25*time.Millisecond)

	assert.Equal(t, int32(2), queueCalls.Load())
	for _, id := range []timebox.AggregateID{queued, scheduled} {
		events, err := s.GetEvents(id, 0)
		assert.NoError(t, err)
		assert.Len(t, events, 1)
	}
	events, err := s.GetEvents(old, 0)
	assert.NoError(t, err)
	assert.Empty(t, events)
	pending, err := s.LoadSchedules(time.Time{}, nil, 10)
	assert.NoError(t, err)
	assert.Empty(t, pending)
}

func TestDeliveryConfig(t *testing.T) {
	b := openBackend(t, testConfig(t))
	err := b.RunDeliveries(t.Context(), nil, tbdbos.DeliveryHandlers{})
	assert.ErrorIs(t, err, tbdbos.ErrDeliveryStoreRequired)
	s := newStore(t, b)
	err = b.RunDeliveries(t.Context(), s, tbdbos.DeliveryHandlers{})
	assert.ErrorIs(t, err, tbdbos.ErrDeliveryHandlersRequired)
}

func TestArchiveQueue(t *testing.T) {
	cfg := testConfig(t)
	cfg.Namespace = "archive"
	b := openBackend(t, cfg)
	s := newStore(t, b)
	client := openClient(t, cfg)

	id := timebox.NewAggregateID("job", "archived")
	event := &timebox.Event{
		Timestamp: time.Now().UTC(),
		Type:      "recorded",
	}
	assert.NoError(t, s.AppendEvents(id, 0, []*timebox.Event{event}))
	assert.NoError(t, s.Archive(id))
	list := func() ([]dbos.WorkflowStatus, error) {
		return dbos.ListWorkflows(client, dbos.WithFilterName("timebox-archive-record"),
			dbos.WithFilterLoadInput(true))
	}
	items, err := list()
	assert.NoError(t, err)
	if !assert.Len(t, items, 1) {
		t.FailNow()
	}
	assert.Equal(t, "timebox-archive", items[0].QueueName)
	assert.Contains(t, items[0].Input, "recorded")

	err = s.ConsumeArchive(t.Context(), func(_ context.Context, rec *timebox.ArchiveRecord) error {
		assert.Equal(t, id, rec.AggregateID)
		assert.Len(t, rec.Events, 1)
		return errRetry
	})
	assert.ErrorIs(t, err, errRetry)

	err = s.ConsumeArchive(t.Context(), func(_ context.Context, rec *timebox.ArchiveRecord) error {
		assert.Equal(t, id, rec.AggregateID)
		assert.Len(t, rec.Events, 1)
		return nil
	})
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		items, err := list()
		return err == nil && len(items) == 1 && items[0].Status == dbos.WorkflowStatusSuccess
	}, 3*time.Second, 50*time.Millisecond)
}

func TestJournalGap(t *testing.T) {
	cfg := testConfig(t)
	cfg.Namespace = "gap"
	b := openBackend(t, cfg)
	client := openClient(t, cfg)
	id := timebox.NewAggregateID("job", "missing")
	key := base64.RawURLEncoding.EncodeToString([]byte(id.Type)) + "/" +
		base64.RawURLEncoding.EncodeToString([]byte(id.Key))
	_, err := dbos.Enqueue[timebox.Empty](client, "test-journal", "record",
		timebox.Empty{}, dbos.WithEnqueueWorkflowID("record/"+key+"/2"))
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	_, err = b.LoadEvents(timebox.LoadEventsRequest{ID: id})
	assert.ErrorIs(t, err, timebox.ErrUnexpectedResult)

	_, err = dbos.Enqueue[timebox.Empty](client, "test-journal", "record",
		timebox.Empty{}, dbos.WithEnqueueWorkflowID("first/"+key))
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	_, err = b.LoadEvents(timebox.LoadEventsRequest{ID: id})
	assert.ErrorIs(t, err, timebox.ErrUnexpectedResult)
}

func openClient(t *testing.T, cfg tbdbos.Config) dbos.Client {
	t.Helper()
	client, err := dbos.NewClient(t.Context(), dbos.ClientConfig{
		DatabaseURL:    cfg.URL,
		DatabaseSchema: cfg.Namespace + "_sdk",
		Logger:         slog.New(slog.DiscardHandler),
	})
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	t.Cleanup(func() { assert.NoError(t, dbos.Shutdown(client, time.Second)) })

	return client
}

func publishWork(t *testing.T, s *timebox.Store, work workArgs) {
	t.Helper()
	msg := &timebox.Message{
		AggregateID: work.queue,
		Type:        "queue",
		Data:        json.RawMessage("{}"),
	}
	err := s.Transact(func(tx *timebox.Transaction) error {
		if err := tx.Publish("jobs", msg); err != nil {
			return err
		}
		return errRetry
	})
	assert.ErrorIs(t, err, errRetry)

	old := &timebox.Message{
		AggregateID: work.old,
		Type:        "scheduled",
		Data:        json.RawMessage("{}"),
	}
	err = s.Transact(func(tx *timebox.Transaction) error {
		if err := tx.Publish("jobs", msg); err != nil {
			return err
		}
		return tx.Schedule("one", work.at.Add(-250*time.Millisecond), old)
	})
	assert.NoError(t, err)

	scheduled := &timebox.Message{
		AggregateID: work.scheduled,
		Type:        "scheduled",
		Data:        json.RawMessage("{}"),
	}
	err = s.Transact(func(tx *timebox.Transaction) error {
		return tx.Schedule("one", work.at, scheduled)
	})
	assert.NoError(t, err)
}

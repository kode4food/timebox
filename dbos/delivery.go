package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/jackc/pgx/v5"

	"github.com/kode4food/timebox"
)

type (
	// DeliveryHandlers process Timebox messages in a transaction from the worker's Store
	DeliveryHandlers struct {
		Queue    func(*timebox.Transaction, timebox.ID, *timebox.Message) error
		Schedule timebox.QueueHandler
	}

	delivery struct {
		Queue    timebox.ID
		Schedule *timebox.Schedule
	}
)

const (
	deliveryAppSuffix    = "-delivery"
	deliveryQueue        = "timebox-delivery"
	deliveryWorkflow     = "timebox-deliver"
	deliveryPollInterval = 100 * time.Millisecond

	// These are the reserved event names carried by the existing Timebox backend contract.
	queueItemAdded        timebox.EventType = "_tb.queue.added"
	queueItemConsumed     timebox.EventType = "_tb.queue.consumed"
	scheduleChanged       timebox.EventType = "_tb.sched.changed"
	scheduleAggregateType timebox.ID        = "_tb.sched_"
	scheduleActiveStatus                    = "_tb.sched.active"
)

var (
	// ErrDeliveryStoreRequired indicates there is no Timebox Store for the DBOS worker
	ErrDeliveryStoreRequired = errors.New("delivery store is required")
	// ErrDeliveryHandlersRequired indicates there are no message handlers for the DBOS worker
	ErrDeliveryHandlersRequired = errors.New("delivery handlers are required")
)

// RunDeliveries runs DBOS queue and schedule workers until ctx ends or the backend closes.
// Bind store to this backend and configure the same handlers on each worker
func (b *Backend) RunDeliveries(
	ctx context.Context, store *timebox.Store, handlers DeliveryHandlers,
) (err error) {
	if store == nil {
		return ErrDeliveryStoreRequired
	}
	if handlers.Queue == nil && handlers.Schedule == nil {
		return ErrDeliveryHandlersRequired
	}
	b.deliverMu.Lock()
	if err := b.checkOpen(); err != nil {
		b.deliverMu.Unlock()
		return err
	}
	b.deliverWG.Add(1)
	b.deliverMu.Unlock()
	defer b.deliverWG.Done()

	runtime, err := b.newRuntime(deliveryAppSuffix)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, dbos.Shutdown(runtime, shutdownTimeout)) }()
	if _, err := dbos.RegisterQueue(runtime, deliveryQueue, dbos.WithPartitionConcurrency(1),
		dbos.WithQueueBasePollingInterval(deliveryPollInterval)); err != nil {
		return err
	}
	dbos.ListenQueues(runtime, deliveryQueue)
	dbos.RegisterWorkflow(runtime, func(ctx dbos.Context, work delivery) (timebox.Empty, error) {
		return deliver(ctx, store, work, handlers)
	}, dbos.WithWorkflowName(deliveryWorkflow))
	if err := dbos.Launch(runtime); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return nil
	}
}

func (b *Backend) enqueueDelivery(tx pgx.Tx, work delivery) error {
	partition := "queue:" + string(work.Queue)
	opts := []dbos.EnqueueOption{
		dbos.WithEnqueueApplicationName(b.cfg.Namespace + deliveryAppSuffix),
		dbos.WithEnqueueTransaction(tx),
	}
	if work.Schedule != nil {
		partition = "schedule:" + string(work.Schedule.Key)
		opts = append(opts, dbos.WithEnqueueDelay(time.Until(work.Schedule.At)))
	}
	opts = append(opts, dbos.WithEnqueueQueuePartitionKey(partition))

	// Use the root context's client API to join the transaction owned by RunAsTransaction.
	_, err := dbos.Enqueue[timebox.Empty](b.runtime, deliveryQueue, deliveryWorkflow, work, opts...)
	return err
}

func deliveries(req *timebox.AppendRequest) []delivery {
	if req.Delete || req.ID == timebox.ArchiveQueueID {
		return nil
	}
	if req.ID.Type == timebox.ArchiveQueueID.Type {
		var res []delivery
		for _, ev := range req.Events {
			if ev.Type == queueItemAdded {
				res = append(res, delivery{Queue: req.ID.Key})
			}
		}
		return res
	}
	if req.ID.Type != scheduleAggregateType || req.Status == nil ||
		*req.Status != scheduleActiveStatus {
		return nil
	}
	for _, ev := range slices.Backward(req.Events) {
		if ev.Type != scheduleChanged {
			continue
		}
		var schedule timebox.Schedule
		if err := json.Unmarshal(ev.Data, &schedule); err == nil && schedule.Message != nil {
			return []delivery{{Schedule: &schedule}}
		}
	}
	return nil
}

func deliver(
	ctx dbos.Context, store *timebox.Store, work delivery, handlers DeliveryHandlers,
) (timebox.Empty, error) {
	if work.Schedule == nil && handlers.Queue == nil {
		return timebox.Empty{}, nil
	}
	if work.Schedule != nil {
		if handlers.Schedule == nil {
			return timebox.Empty{}, nil
		}
		// DBOS rounds its wake-up times to milliseconds.
		delay := max(time.Until(work.Schedule.At)+time.Millisecond, time.Millisecond)
		if _, err := dbos.Sleep(ctx, delay); err != nil {
			return timebox.Empty{}, err
		}
	}
	return dbos.RunAsStep(ctx, func(ctx context.Context) (timebox.Empty, error) {
		if work.Schedule != nil {
			return timebox.Empty{}, deliverSchedule(store, work.Schedule, handlers.Schedule)
		}
		return timebox.Empty{}, deliverQueue(ctx, store, work.Queue, handlers.Queue)
	}, dbos.WithStepMaxRetries(3))
}

func deliverSchedule(
	store *timebox.Store, work *timebox.Schedule, handler timebox.QueueHandler,
) error {
	return store.Transact(func(tx *timebox.Transaction) error {
		active, err := tx.LoadSchedule(work.Key)
		if err != nil || active == nil {
			return err
		}
		if !active.At.Equal(work.At) || !active.Message.Equal(work.Message) {
			return nil
		}
		active, err = tx.ConsumeSchedule(work.Key, work.At)
		if err != nil || active == nil {
			return err
		}
		return handler(tx, active.Message)
	})
}

func deliverQueue(
	ctx context.Context, store *timebox.Store, queue timebox.ID,
	handler func(*timebox.Transaction, timebox.ID, *timebox.Message) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, deliveryPollInterval)
	defer cancel()

	called := false
	err := store.Consume(ctx, queue, func(tx *timebox.Transaction, msg *timebox.Message) error {
		called = true
		return handler(tx, queue, msg)
	})
	if !called && errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

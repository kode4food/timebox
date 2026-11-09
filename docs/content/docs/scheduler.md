---
title: "Scheduler"
weight: 40
---

# Scheduler

Use the scheduler for work that must happen later and survive a process restart. For example, an order can schedule an expiry check when it is placed. The schedule lives in the same backend as the order, and `Transaction.Schedule` commits it atomically with the order's events.

A schedule carries a `timebox.Message`: a type, an aggregate ID, and optional data. The message tells your application what to do when the deadline arrives. In the example below, `order.expire` requests a check, and the `order.expired` event is raised only when the order is still pending. `timebox.Event` embeds `Message` and adds the sequence and timestamp of a recorded event.

## Schedule the Check

Assume `store` is a `*timebox.Store`, `orders` is an executor for `OrderState`, and `placeOrder` is your order command:

```go
key := timebox.ScheduleKey("order/ORD-12345/expire")
deadline := time.Now().Add(30 * time.Minute)
err := store.Transact(func(tx *timebox.Transaction) error {
	if _, err := tx.Exec(orders, orderID, placeOrder); err != nil {
		return err
	}
	return tx.Schedule(key, deadline, &timebox.Message{
		AggregateID: orderID,
		Type:        "order.expire",
	})
})
```

The message needs no data because its `AggregateID` identifies the order. Calling `tx.Schedule` again with the same key replaces its message and deadline. Scheduling the same deadline and message again is a no-op. If payment completes first, the check still arrives and finds the order paid, so the order stays as it is.

`tx.LoadSchedule(key)` reads the active schedule as part of the transaction, and the commit is checked against what it read. A transaction can therefore schedule a key exactly when it is free, and a concurrent writer reruns against the schedule that won:

```go
err := store.Transact(func(tx *timebox.Transaction) error {
	active, err := tx.LoadSchedule(key)
	if err != nil || active != nil {
		return err
	}
	return tx.Schedule(key, deadline, msg)
})
```

## Deliver the Message

Run a `scheduler.Scheduler` in a long-lived process using the same durable backend. Its processor receives each due message and a transaction. Check current state and raise any resulting event in that transaction:

```go
runner, err := scheduler.New(scheduler.Config{
	Store: store,
	Processor: func(
		tx *timebox.Transaction, msg *timebox.Message,
	) error {
		if msg.Type != "order.expire" {
			return fmt.Errorf("unexpected message %q", msg.Type)
		}
		_, err := tx.Exec(orders, msg.AggregateID,
			func(
				order OrderState,
				ag *timebox.Aggregator[OrderState],
			) error {
				if order.Status != "pending" {
					return nil
				}
				return ag.Raise("order.expired", timebox.Empty{})
			},
		)
		return err
	},
})
if err != nil {
	return err
}
return runner.Run(ctx)
```

`Run` blocks until its context ends. Schedules persist in the backend, so a scheduler recreated after a restart picks them up. The runner consumes the schedule in the processor's transaction, so the consumption and the processor's state changes commit together when the processor returns nil. A concurrent replacement aborts the commit, and the runner delivers the replacement at its own deadline. An error leaves the schedule active for retry. Return `scheduler.ErrRetry` to retry quietly when a delay is expected. The processor can run more than once after a transaction conflict, so put external I/O in `OnSuccess`.

The runner watches local schedule commits and rescans the backend for changes made by other stores or processes. `RescanInterval` and `RetryDelay` both default to one second. Call `runner.Wake()` when an external change needs an immediate rescan. Use `tx.LoadSchedule(key)` to inspect one active schedule and `store.ListSchedules(through)` to list active schedules due by a time. `time.Time{}` lists all. For recurring work, schedule the next occurrence under the same key in the processor's transaction.

## Deliver from Your Own Loop

`tx.ConsumeSchedule(key, through)` removes and returns the schedule under `key` if it is due by `through`, or returns nil when nothing is due. It commits with the rest of the transaction, so a delivery loop of your own can act on the message atomically:

```go
err := store.Transact(func(tx *timebox.Transaction) error {
	due, err := tx.ConsumeSchedule(key, tx.Now())
	if err != nil || due == nil {
		return err
	}
	return handle(tx, due.Message)
})
```

Combine it with `store.ListSchedules(through)` to find keys worth trying, and with `store.ScheduleChanges()` to learn when local commits change the set.

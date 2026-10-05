---
title: "Scheduler"
weight: 40
---

# Scheduler

Use the scheduler for work that must happen later and survive a process restart. For example, an order can schedule an expiry check when it is placed. The schedule lives in the same backend as the order, and `Transaction.Schedule` commits it atomically with the order's events.

A schedule carries a `timebox.Message`: a type, an aggregate ID, and optional data. The message tells your application what to do when the deadline arrives. In the example below, `order.expire` requests a check; the `order.expired` event is raised only when the order is still pending. `timebox.Event` embeds `Message` and adds the sequence and timestamp of a recorded event.

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

The message needs no data because its `AggregateID` identifies the order. Calling `tx.Schedule` again with the same key replaces its message and deadline. If payment completes first, call `tx.CancelSchedule(key)` in the payment transaction. `tx.CancelSchedulePrefix(prefix)` cancels all active keys under a prefix.

## Deliver the Message

Run a `scheduler.Scheduler` in a long-lived process using the same durable backend. Its emitter receives each due delivery. Check current state, raise any resulting event, and consume the delivery in one transaction:

```go
runner, err := scheduler.New(scheduler.Config{
	Store: store,
	Emitter: func(
		_ context.Context, delivery *scheduler.Delivery,
	) error {
		msg := delivery.Message()
		if msg.Type != "order.expire" {
			return fmt.Errorf("unexpected message %q", msg.Type)
		}
		return store.Transact(func(tx *timebox.Transaction) error {
			_, err := tx.Exec(orders, msg.AggregateID,
				func(
					order OrderState,
					ag *timebox.Aggregator[OrderState],
				) error {
					if order.Status != "pending" {
						return nil
					}
					return ag.Raise("order.expired", struct{}{})
				},
			)
			if err != nil {
				return err
			}
			return delivery.Consume(tx)
		})
	},
})
if err != nil {
	return err
}
return runner.Run(ctx)
```

`Run` blocks until its context ends. The scheduler can be recreated after a restart because schedules remain in the backend. `Consume` checks the schedule version, so a delivery from before a replacement or cancellation cannot consume the newer schedule. If the emitter fails or leaves a schedule active, the runner retries it; make external effects idempotent because delivery may happen more than once.

The runner watches local schedule commits and rescans the backend for changes made by other stores or processes. `RescanInterval` and `RetryDelay` both default to one second. Call `runner.Wake()` when an external change needs an immediate rescan. Use `store.LoadSchedule(key)` to inspect one active schedule and `store.ListSchedules(through)` to list active schedules due by a time; `time.Time{}` lists all. For recurring work, consume a delivery and schedule its next occurrence with the same key in one transaction.

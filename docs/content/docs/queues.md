---
title: "Queues"
weight: 41
---

# Queues

Use a queue for work that must survive a process restart and be handled by whoever picks it up next, in order. `Transaction.Publish` commits a message to a named queue atomically with other aggregate events. `Store.Consume` blocks until the oldest unconsumed message is handled.

A queue is identified by a `timebox.ID` name. Published messages are `timebox.Message` values: a type, an aggregate ID, and optional data.

## Publish a Message

Assume `store` is a `*timebox.Store` and `orders` is an executor for `OrderState`:

```go
err := store.Transact(func(tx *timebox.Transaction) error {
	if _, err := tx.Exec(orders, orderID, placeOrder); err != nil {
		return err
	}
	return tx.Publish("order.placed", &timebox.Message{
		AggregateID: orderID,
		Type:        "order.notify",
	})
})
```

Each call adds a new message to the end of the queue. Messages are delivered in the order they were published.

## Consume a Message

```go
err := store.Consume(ctx, "order.placed",
	func(tx *timebox.Transaction, msg *timebox.Message) error {
		if msg.Type != "order.notify" {
			return fmt.Errorf("unexpected message %q", msg.Type)
		}
		return notifyCustomer(tx, msg.AggregateID)
	},
)
```

`Consume` blocks until a message is available or `ctx` ends, then commits the handler's work and the message's removal together. The message is removed when the handler returns nil. An error leaves the message in place for the next `Consume` call, so make external I/O idempotent. Call `Consume` in a loop to drain a queue continuously.

Within one process, a publish wakes a blocked `Consume` immediately. Across processes sharing a backend, it wakes within about a second.

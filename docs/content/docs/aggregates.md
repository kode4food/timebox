---
title: "Aggregates and Events"
weight: 20
---

# Aggregates and Events

## Aggregate IDs

Every aggregate ID contains a type and key:

```go
orderID := timebox.NewAggregateID("order", "ORD-123")
```

Both components are always populated. For a concept with exactly one aggregate, use `NewAggregateType`; it fills the key with `timebox.SingletonKey`:

```go
catalogID := timebox.NewAggregateType("catalog")
```

Use this pattern for singleton concepts such as a catalog or cluster. Use `NewAggregateID` when a type has many instances, such as `("workflow", workflowID)`.

## Event Types and Payloads

Event type names are durable identifiers. Payloads are JSON encoded by `Aggregator.Raise`:

```go
const OrderConfirmed timebox.EventType = "order.confirmed"

err := ag.Raise(OrderConfirmed, timebox.Empty{})
```

Use `timebox.Empty{}` for an event without payload fields.

Use `Event.GetValue` when reading an event directly:

```go
data, err := event.GetValue[OrderCreatedData]()
```

Timebox caches a compatible decoded value, including the value supplied to a newly raised event.

## Message Handlers

`Handler`, `MakeHandler`, and `MakeDispatcher` work with `Message`, so the same dispatcher can route recorded event messages and deferred schedule messages by type. An `Event` embeds its message; pass that field to a handler or dispatcher:

```go
dispatch := timebox.MakeDispatcher(handlers)
err := dispatch(&event.Message)
err = dispatch(scheduledMessage)
```

Use `MakeHandler` when a handler needs a decoded payload. Event appliers remain separate because they can also use recorded metadata such as the event timestamp.

## Appliers

An applier maps the previous state and one event to the next state. Prefer `MakeApplier` for typed payloads:

```go
func orderCreated(
    order Order, event *timebox.Event, data OrderCreatedData,
) Order {
    order.CustomerName = data.CustomerName
    order.CreatedAt = event.Timestamp
    return order
}

var orderAppliers = timebox.Appliers[Order]{
    OrderCreated: timebox.MakeApplier(orderCreated),
}
```

Appliers run both while replaying stored history and immediately after `Raise`. They should not perform I/O or other side effects.

## Post-Commit Actions

Side effects belong after a successful commit. Register defaults when constructing an executor, or register a command-specific action through the aggregator:

```go
ag.OnSuccess(func(order Order, events []*timebox.Event) {
    publish(order, events)
})
```

Success actions run only after persistence succeeds. Timebox recovers a panic in an action and logs it; the commit is already durable and is not rolled back.

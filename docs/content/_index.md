---
title: "Timebox"
---

# Timebox

Timebox is a small, opinionated event-sourcing library for Go. It stores immutable aggregate events, rebuilds typed state, protects writes with optimistic concurrency, and keeps transactions atomic across aggregates.

## What It Provides

- Typed aggregate state and event appliers
- Automatic retries on optimistic concurrency conflicts
- Atomic multi-aggregate transactions
- Snapshots and optional event trimming
- Status and tag indexes updated with the event append
- Memory, PostgreSQL, Redis/Valkey, and Raft backends
- One-way archive storage in memory, Redis, and Raft

## Quick Start

```sh
go get github.com/kode4food/timebox@v0.1.0
```

Start with the in-memory backend, define a state constructor and appliers, then execute a command:

```go
backend := memory.Open()
defer backend.Close()

store, err := backend.NewStore()
if err != nil {
    return err
}

orders := store.Executor(newOrder, orderAppliers)
orderID := timebox.NewAggregateID("order", "ORD-123")

order, err := orders.Exec(orderID,
    func(_ Order, ag *timebox.Aggregator[Order]) error {
        return ag.Raise(OrderCreated, OrderCreatedData{
            CustomerName: "Ada",
        })
    },
)
```

The complete runnable [order example](https://github.com/kode4food/timebox/blob/main/examples/order.go) shows an aggregate lifecycle using Redis.

→ [Build the order tutorial]({{< relref "/docs/tutorial" >}})

## Status

Timebox is 0.x software. Its persistence contract is tested across every backend, but public APIs and durable formats may still change between minor releases.

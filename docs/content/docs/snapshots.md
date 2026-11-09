---
title: "Snapshots"
weight: 50
---

# Snapshots

A snapshot stores an aggregate's projected state at a particular event sequence. When an executor loads that aggregate, it starts from the snapshot and applies only later events. The event log remains the source of truth.

## When snapshots are saved

An executor saves a snapshot while loading an aggregate if it finds events after the current snapshot and either no snapshot exists or the trailing event payloads are larger than the snapshot payload multiplied by `SnapshotRatio`. The default ratio is `1.0`. A larger ratio means fewer automatic snapshots.

```go
store, err := backend.NewStore(timebox.Config{
    SnapshotRatio: 2.0,
})
```

To save one explicitly, use the executor that owns the aggregate's state and appliers:

```go
err := orderExecutor.SaveSnapshot(orderID)
```

`SaveSnapshot` loads the current state and writes it at the corresponding event sequence. `Store.PutSnapshot` is the lower-level API when you already have both the state and its sequence.

## Trimming old events

By default, snapshots leave the event log intact. Set `TrimEvents` to discard events preceding a saved snapshot:

```go
store, err := backend.NewStore(timebox.Config{
    TrimEvents: true,
})
```

The executor loads the aggregate from its snapshot and later events, and `GetEvents` returns the history after the snapshot. Leave trimming off for aggregates whose full history you audit, replay from the beginning, or export. Trimming happens each time a snapshot is saved.

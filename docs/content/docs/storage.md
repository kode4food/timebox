---
title: "Snapshots, Indexing, and Archiving"
weight: 50
---

# Snapshots, Indexing, and Archiving

## Snapshots

Snapshots accelerate state loading; events remain authoritative. Executors save snapshots opportunistically when there is no snapshot or when trailing event data exceeds `SnapshotRatio` relative to the stored snapshot.

Set `TrimEvents` when retained events before a snapshot are unnecessary:

```go
store, err := backend.NewStore(timebox.Config{
    TrimEvents:    true,
    SnapshotRatio: 1.0,
})
```

Trimming happens with snapshot persistence. Do not enable it when the complete live event history must remain queryable.

## Status and Tag Indexing

An indexer derives metadata from the same event batch being appended:

```go
func orderIndexer(events []*timebox.Event) []*timebox.Index {
    indexes := make([]*timebox.Index, 0, len(events))
    for _, event := range events {
        switch event.Type {
        case OrderCreated:
            indexes = append(indexes, &timebox.Index{
                Status: new("active"),
            })
        case OrderDelivered:
            indexes = append(indexes, &timebox.Index{
                Status: new("delivered"),
            })
        }
    }
    return indexes
}

store, err := backend.NewStore(timebox.Config{Indexer: orderIndexer})
```

The last non-nil status wins. Tag values add (`true`) or remove (`false`) membership. Query the derived indexes through:

```go
status, err := store.GetAggregateStatus(orderID)
active, err := store.ListAggregatesByStatus("active")
priority, err := store.ListAggregatesByTag("priority")
orders, err := store.ListAggregates("order")
```

A workflow service can use an indexer to turn started and deactivated events into queryable status and tag records.

## Archiving

Memory, Redis, and Raft can move a snapshot and event history into archive storage, then remove the live aggregate and its indexes:

```go
if err := store.Archive(orderID); err != nil {
    return err
}
```

Consume one archive record with a context deadline:

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

err := store.ConsumeArchive(ctx,
    func(ctx context.Context, record *timebox.ArchiveRecord) error {
        return writeArchive(ctx, record)
    },
)
```

Consumption is at least once, so handlers must be idempotent. Archiving is one-way; Timebox does not restore archived aggregates.

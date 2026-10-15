---
title: "Archiving"
weight: 52
---

# Archiving

Archiving removes an aggregate from live storage and queues its stored snapshot and events for export. Use it when an aggregate has finished its lifecycle and should no longer appear in live queries, but its stored record still needs to be retained elsewhere.

Each backend atomically bundles an aggregate's snapshot and remaining events into the shared archive queue, then removes the live record. `ConsumeArchive` drains that queue on top of `Store.Consume`.

## Archive an aggregate

```go
if err := store.Archive(orderID); err != nil {
    return err
}
```

The backend moves the aggregate's stored snapshot and events into its archive queue, then removes the live aggregate and its indexes. This is one-way: Timebox does not restore archived aggregates. If `TrimEvents` was enabled, events already trimmed before archiving are not in the archive record. The snapshot holds the state at its saved sequence.

## Export archive records

`ConsumeArchive` waits for one record, calls the handler, and returns. Call it repeatedly in a worker to drain the queue:

```go
for {
    err := store.ConsumeArchive(ctx,
        func(ctx context.Context, record *timebox.ArchiveRecord) error {
            return writeArchive(ctx, record)
        },
    )
    if err != nil {
        return err
    }
}
```

An `ArchiveRecord` contains the aggregate ID, snapshot data and sequence, and remaining events. A handler error leaves the record available for another attempt. A successful handler can also be called again if acknowledgement fails, so make the export idempotent. Use the aggregate ID as the destination key. Do not discard the snapshot when the earlier events have been trimmed.

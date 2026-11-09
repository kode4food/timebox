---
title: "Archiving"
weight: 52
---

# Archiving

Archiving removes an aggregate from live storage and queues its stored snapshot and events for export. Use it for an aggregate that has finished its lifecycle and whose record belongs in long-term storage.

`Archive` hands an aggregate's snapshot and remaining events to an archive queue and removes the live record in one step. `ConsumeArchive` drains that queue.

## Archive an aggregate

```go
if err := store.Archive(orderID); err != nil {
    return err
}
```

The aggregate and its indexes leave live storage once the archive record is queued. Archiving is one-way. With `TrimEvents` enabled, the record holds the events after the last snapshot, and the snapshot holds the state at its saved sequence.

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

An `ArchiveRecord` contains the aggregate ID, snapshot data and sequence, and remaining events. A handler error leaves the record for another attempt. A successful handler can run again if acknowledgement fails, so make the export idempotent, keyed by the aggregate ID. Keep the snapshot whenever earlier events were trimmed.

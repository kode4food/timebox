---
title: "Indexing"
weight: 51
---

# Indexing

An indexer turns committed events into a current status and a set of tags for each aggregate. Use these to find aggregates without loading and replaying every event stream. For example, you can query for active orders or orders awaiting review.

Supply an `Indexer` when creating the store. It receives each event batch being appended and returns index updates derived from that batch:

```go
func orderIndexer(events []*timebox.Event) []*timebox.Index {
    var indexes []*timebox.Index
    for _, event := range events {
        switch event.Type {
        case OrderCreated:
            indexes = append(indexes, &timebox.Index{
                Status: new("active"),
            })
        case OrderDelivered:
            indexes = append(indexes, &timebox.Index{
                Status: new("delivered"),
                Tags:   map[string]bool{"awaiting-review": true},
            })
        case OrderReviewed:
            indexes = append(indexes, &timebox.Index{
                Tags: map[string]bool{"awaiting-review": false},
            })
        }
    }
    return indexes
}

store, err := backend.NewStore(timebox.Config{Indexer: orderIndexer})
```

The status and tag changes are committed atomically with the event batch. A nil `Status` leaves the current status alone; an empty string clears it. For a tag, `true` adds membership and `false` removes it. If a batch yields multiple updates for the same status or tag, the last one wins.

Query the resulting indexes through the store:

```go
status, err := store.GetAggregateStatus(orderID)
active, err := store.ListAggregatesByStatus("active")
review, err := store.ListAggregatesByTag("awaiting-review")
```

`GetAggregateStatus` returns an empty string when no status is set. `ListAggregatesByStatus` returns IDs and the timestamp of the last event in the batch that updated each status; `ListAggregatesByTag` returns IDs. The indexer should derive its result from the supplied events so retries produce the same updates.

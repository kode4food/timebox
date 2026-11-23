---
title: "Indexing"
weight: 51
---

# Indexing

An indexer turns committed events into a current status and a set of tags for each aggregate. Use them to find aggregates by status or tag, such as active orders or orders awaiting review.

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

The status and tag changes are committed atomically with the event batch. A nil `Status` leaves the current status alone, and an empty string clears it. For a tag, `true` adds membership and `false` removes it. If a batch yields multiple updates for the same status or tag, the last one wins.

Query the resulting indexes through the store:

```go
status, err := store.GetAggregateStatus(orderID)
active, err := store.ListAggregatesByStatus(timebox.StatusQuery{
	Status: "active",
	Type:   "order",
	Limit:  100,
})
tags, err := store.GetAggregateTags(orderID)
```

`GetAggregateStatus` returns an empty string when no status is set. `ListAggregatesByStatus` takes a `StatusQuery` and returns one page of IDs with the timestamp of the last event in the batch that updated each status, newest first. `Type` selects the aggregate type, `Through`, when set, bounds by status time, and `Limit` is the page size, at least one. Passing the last entry of a page as `After` resumes past it. `GetAggregateTags` returns the current tags of each aggregate it's given, in one read, keyed by aggregate ID. Derive the result from the supplied events alone, so retries produce the same updates.

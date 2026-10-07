# Changelog

Notable changes to Timebox.

## 0.3

### Scheduling

- `scheduler.Scheduler` delivers durable deferred messages, surviving process
  restarts and running against the same backend as its aggregates
- `Transaction.Schedule`, `CancelSchedule`, `CancelSchedulePrefix`, and
  `ConsumeSchedule` commit schedule changes atomically with aggregate events
- `Store.LoadSchedule` and `Store.ListSchedules` query active schedules

### Event sourcing

- `Event` is now a recorded `Message` (type, aggregate ID, data) with a
  sequence and timestamp; `Handler`, `MakeHandler`, and `MakeDispatcher`
  operate on `Message` instead of `Event`
- Added `Empty` for payload-less messages and events

### Indexing

- `ListAggregatesByStatus` takes a `StatusQuery`, narrowing by aggregate type,
  key prefix, and a latest status time instead of status alone

## 0.2

### Event sourcing

- Committed-event publishing for every backend through `timebox.Publisher`
- Exported `Constructor` type for executor state constructors

### Persistence

- Memory backend accepts an optional `Config`
- `raft.Publisher` replaced by `timebox.Publisher`

### Removed

- `Executor.GetStore` and `Executor.AppliesEvent`

### Documentation

- Documentation site with guides and a runnable tutorial example

## 0.1

First public release.

### Event sourcing

- Append-only aggregate event logs with optimistic concurrency
- Typed aggregate executors with event appliers, command retries, and success actions
- Atomic transactions across multiple aggregates
- Explicit and opportunistic snapshots, with optional event trimming

### Indexing

- Append-time aggregate status and tag indexing
- Queries by aggregate type, status, and tag

### Persistence

- In-process memory backend
- PostgreSQL backend with schema-managed event, snapshot, status, and tag storage
- Redis and Valkey backend with Lua-backed atomic operations
- Replicated Raft backend with a durable segmented log, recovery, compaction, and snapshot transfer

### Archiving

- One-way archive storage for memory, Redis, and Raft
- At-least-once archive consumption with idempotent handlers

### Scope

- Requires Go 1.27 or newer
- PostgreSQL does not support archiving

# Changelog

Notable changes to Timebox.

## 0.4

### Event sourcing

- Executors accept a `Reducer`, with `Appliers.Reduce` as the default
- `Config.Clock` sets the Store's time source, exposed as `Transaction.Now`

### Persistence

- `AppendRequest.Delete` removes an aggregate atomically, keeping its sequence
- Faster PostgreSQL appends and reads

### Scheduling

- `Transaction.LoadSchedule` reads a schedule within a Transaction
- `ConsumeSchedule(key, through)` removes and returns a due schedule
- Rescheduling the same deadline and message is a no-op

### Queues

- Durable named queues through `Transaction.Publish` and `Store.Consume`

### Archiving

- Every backend, including PostgreSQL, supports archiving through `Store.ConsumeArchive`

### Removed

- `Transaction.CancelSchedule` and `CancelSchedulePrefix`
- `Schedule.Version`, `ScheduleVersion`, and `ScheduleVersionConflictError`
- `TrimEvents` on `AppendRequest`, `LoadEventsRequest`, and `LoadSnapshotRequest`
- `Store.LoadSchedule`

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

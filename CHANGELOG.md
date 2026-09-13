# Changelog

Notable changes to Timebox.

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

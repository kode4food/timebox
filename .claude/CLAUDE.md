# Timebox

Event-sourcing persistence library for Go. Provides append-only aggregate event storage with optimistic concurrency, snapshots, status/label indexing, and pluggable backends. The `raft` sub-package adds multi-node consensus via etcd raft + bbolt; the `redis` sub-package provides a Redis-backed backend.

## Build & Test

```bash
make test        # vet + staticcheck + go test ./...
make check       # vet + staticcheck only
make format      # goimports + go fix
make pre-commit  # format + test
```

## Package Structure

```
timebox (root)   # Core interfaces and Store: Append, Snapshot, events, indexing
raft/            # etcd raft + bbolt persistence backend
redis/           # Redis persistence backend
examples/        # Usage examples (own go.mod)
```

## Backends

### raft/

Multi-node consensus backend. Each node runs etcd raft + bbolt. Followers propose directly via `node.Propose()` — raft handles forwarding internally.

Key types: `Persistence`, `Config`, `Server`

### redis/

Single-node Redis backend with Lua-scripted atomic appends, archiving support, and indexed queries.

Key types: `Persistence`, `Config`

## Before Committing

```bash
make pre-commit
```

## Testing Requirements

- Minimum 90% test coverage
- Black-box tests only (`package_test` suffix)

## Code Quality

- No magic numbers - use named constants
- Prefer simple solutions over abstractions
- Only make changes directly requested
- Read files before editing them

## Backward Compatibility

This project is in active development. Do not preserve backward compatibility, avoid deprecation paths, and prefer breaking changes when they simplify the system.

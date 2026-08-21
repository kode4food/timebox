# Timebox

## Project

Event-sourcing persistence library for Go. Module: `github.com/kode4food/timebox`.

Timebox provides append-only aggregate event storage with optimistic concurrency, snapshots, status and label indexing, archiving, and pluggable memory, PostgreSQL, Redis, and Raft backends.

## Exact Scope

- Implement the requested change and preserve everything outside its explicit scope.
- Never rename, move, redesign, delete, inline, add compatibility, or perform adjacent cleanup unless the request requires it.
- Explain required broad consequences before acting.
- Complete the whole requested boundary. Never shrink or reframe the request.
- This project is in active development. Do not add deprecation paths or compatibility layers unless explicitly requested.

## Architecture

Dependencies flow from the public Timebox API toward backend implementations and narrow internal packages.

- `timebox`: public event, aggregate, store, executor, indexing, snapshot, and backend contracts.
- `memory`: in-process backend.
- `postgres`: PostgreSQL backend and schema ownership.
- `redis`: Redis/Valkey backend and Lua-backed atomic operations.
- `raft`: replicated commit path, durable Raft log, transport, and local materialized state.
- `internal/binary`: shared binary encoding primitives.
- `internal/compliance`: backend contract tests.
- `internal/id`: internal identifier support.

Rules:

1. Give each concept one authoritative owner.
2. Keep exported signatures in the language of the receiving public package.
3. Preserve the core append contract in every backend: aggregate-local optimistic concurrency, atomic event-batch append, and aligned derived index updates.
4. Treat snapshots as accelerative state, never as the authoritative event source.
5. Add interfaces only for real lifecycle or substitution seams.
6. Name packages and files by concern. Never create `util`, `helpers`, `common`, `models`, or `types` dumping grounds.
7. Group files by concern. Size alone never justifies a split or move.
8. Put reusable backend semantics in the root contract or compliance suite rather than duplicating them across backends.
9. Move code only after confirming its owner and reducing caller concepts. Forwarding wrappers never justify a boundary.

## Args and Results

- Functions with 5 or more arguments use an `Args` struct.
- Functions returning 3 or more data values use a `Res` struct. A trailing `bool` or `error` does not count as data.
- Name same-typed values with distinct roles. Add a struct when call sites become clearer.
- Declare single-use `Args` or `Res` immediately before its function. Pass it by value and never store or forward it.
- Give multi-call-site structs descriptive names without `Args` or `Res`.
- Pass plain structs up to 32 bytes by value and structs over 64 bytes by pointer. Use pointers for identity or mutation.

## Go Style

### Naming

- Use one lowercase receiver letter based on the type name or role.
- Use short nearby locals: `ok` for lookups and assertions, `err` for errors, and longer names at API boundaries.
- Use verb-noun functions. Use `New` constructors, normally returning pointers.
- Omit type arguments Go can infer. Keep result-only or otherwise non-inferable arguments.
- Keep acronyms uppercase: `ID`, `URL`, `HTTP`.
- Put `Err`-prefixed sentinels in an error `var` block.
- Avoid redundant `is` or `has` prefixes on local booleans.
- Follow analogous standard-library names.

### Declarations and Files

- Declare all `type` and `const` values at package scope, including tests.
- Put types before other declarations in the owning concern file.
- Order top-level declarations as types, constants, variables, exported constructors, exported methods, exported functions, unexported methods, then unexported functions.
- Keep related methods together in call-chain order.
- Run `goimports` on Go files.
- Keep Go code and comments within 80 columns.
- Do not hard-wrap Markdown prose.

### Structs and Control Flow

- Use named fields in struct literals except single-field wrappers.
- Prefer guard clauses. Keep conditional nesting to one level unless an early return duplicates work.
- Use multi-assignment only for multiple results from one call.
- Store mutable counters, caches, registries, and lifecycle state on their owning structs.
- Reference another package's exported value directly; never re-export it through a package-level variable.

### Comments

- Give every exported declaration Godoc of at most 3 lines describing what it is or does.
- Comment unexported declarations only for non-obvious reasons, using at most 2 lines.
- End Godoc without a period.
- Use inline comments for why only.

### Errors

- Use typed package-level sentinels and wrap them as `%w: context`.
- Handle errors immediately.
- Panic only in documented `Must...` helpers whose contract promises it.

## Persistence

- Preserve existing data unless a destructive migration is explicit and separately authorized.
- Keep durable format readers and writers aligned. Verify restart, replay, compaction, and snapshot paths after format or dependency changes.
- Keep retries idempotent and partial failures observable.
- Archiving is one-way: move the snapshot and event history to archive storage, then clear live records only after the archive write succeeds.
- Do not weaken Raft log ordering, fsync, recovery, compaction, or snapshot-transfer behavior without focused tests.

## Testing

- Maintain at least 90% coverage.
- Use external `package_test` packages.
- Use `testify/assert` only and omit assertion messages.
- Keep test names short. Put scenarios in concise `t.Run` names.
- Use table-driven tests for multiple scenarios and call `t.Helper()` in helpers.
- Put shared backend behavior in `internal/compliance`.
- Completion requires `make test`. If an external service is unavailable, run the focused package tests and report the environmental failure.

## Build

```bash
make format
make check
make test
make pre-commit
```

## Git

- Preserve the reviewed index exactly.
- Never commit unless the current request explicitly asks.
- Never stage, unstage, reset, restore, or stash unless explicitly asked.

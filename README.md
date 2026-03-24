# Timebox <img src="./docs/logo.png" align="right" height="100"/>

![Build Status](https://github.com/kode4food/timebox/actions/workflows/build.yml/badge.svg) [![Code Coverage](https://qlty.sh/gh/kode4food/projects/timebox/coverage.svg)](https://qlty.sh/gh/kode4food/projects/timebox) [![Maintainability](https://qlty.sh/gh/kode4food/projects/timebox/maintainability.svg)](https://qlty.sh/gh/kode4food/projects/timebox) [![GitHub](https://img.shields.io/github/license/kode4food/timebox)](https://github.com/kode4food/timebox/blob/main/LICENSE.md)

Timebox is a small, opinionated event sourcing library for Go with pluggable persistence backends including Redis/Valkey, PostgreSQL, and a Raft backend. It provides an append-only event log, optimistic concurrency, snapshotting, and append-time indexing so multiple instances can coordinate through the same store.

## Backends

Timebox currently ships with:

- `memory` for tests and single-process use
- `redis` for Redis or Valkey deployments
- `postgres` for PostgreSQL-backed persistence
- `raft` for multi-node consensus

## Core Concepts

- `Store`: event-store semantics over a `Persistence`
- `Executor`: loads aggregate state, runs a command, persists raised events, and retries on optimistic conflicts
- `Aggregator`: accumulates events and exposes the current aggregate view during a command
- `Indexer`: optional append-time hook that derives status and label updates from an appended event batch
- `Snapshot`: cached aggregate state plus the sequence it represents

## Store Behavior

`timebox.Config` controls store behavior regardless of backend:

- `Snapshot`: background snapshot worker settings and trim policy
- `MaxRetries`: optimistic concurrency retry limit
- `CacheSize`: executor projection cache size
- `Archiving`: enables archive APIs
- `Indexer`: optional function that derives status and label updates from an appended event batch

Create a store either by using a backend helper such as `postgres.NewStore(cfg)` or by calling `timebox.NewStore(persistence, cfg)` directly.

## Backend Config

### Postgres

`postgres.Config` adds:

- `URL`: connection URL
- `Prefix`: logical store namespace
- `MaxConns`: pgx pool size cap
- `Timebox`: embedded `timebox.Config`

The Postgres backend stores:

- aggregate status and labels in `timebox_index`
- snapshots in `timebox_snapshot`
- events in `timebox_events`

### Redis

`redis.Config` adds:

- `Addr`: Redis or Valkey host:port
- `Password`: optional password
- `Prefix`: logical store namespace
- `Shard`: optional hash-tag value for cluster slot affinity
- `DB`: logical database index
- `Timebox`: embedded `timebox.Config`

### Raft

`raft.Config` fields:

- `Timebox`: embedded `timebox.Config` used to configure the store layer
- `LocalID`: stable local Raft node ID
- `Address`: node address used for Raft traffic
- `DataDir`: durable local state directory
- `Servers`: bootstrap voter set

## Indexing

`Config.Indexer` lets you derive indexed metadata from an appended event batch. `Index` currently supports:

- `Status`: aggregate status plus the time it entered that status
- `Labels`: current aggregate label values

Read paths exposed by the store:

- `Store.GetAggregateStatus(id)`
- `Store.ListAggregatesByStatus(status)`
- `Store.ListLabelValues(label)`
- `Store.ListAggregatesByLabel(label, value)`

## Archiving

Archiving moves an aggregate's snapshot and event history into backend-specific archive storage and clears the live records. It is a one-way operation. The `memory` and `redis` backends support archiving, while `postgres` and `raft` do not.

Enable it with `Archiving`, then call `Store.Archive(id)`.

To consume archived records, call `Store.ConsumeArchive(ctx, handler)`. It blocks until one record is processed or the context is done. Use `context.WithTimeout` to poll with a deadline.

Handlers must be idempotent because processing is at-least-once.

## Examples

- `examples/order.go` shows a simple order lifecycle over Timebox

## Status

Work in progress. Not ready for production use.

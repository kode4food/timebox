# Timebox <img src="./docs/logo.png" align="right" height="100"/>

![Build Status](https://github.com/kode4food/timebox/actions/workflows/build.yml/badge.svg) [![Code Coverage](https://qlty.sh/gh/kode4food/projects/timebox/coverage.svg)](https://qlty.sh/gh/kode4food/projects/timebox) [![Maintainability](https://qlty.sh/gh/kode4food/projects/timebox/maintainability.svg)](https://qlty.sh/gh/kode4food/projects/timebox) [![GitHub](https://img.shields.io/github/license/kode4food/timebox)](https://github.com/kode4food/timebox/blob/main/LICENSE.md)

Timebox is a small, opinionated event sourcing library for Go backed by Redis or Valkey. It provides an append-only event log, optimistic concurrency, snapshotting, and an in-process event hub so multiple instances can coordinate through the same store.

## Features

- **Complete event sourcing** with immutable event log and sequence-based versioning
- **Optimistic concurrency** with automatic retries on conflicts
- **Snapshots and caching** with background workers and LRU projection cache
- **Indexing** through append-time projections for status and labels
- **Distributed coordination** through a shared Redis/Valkey backend
- **Archiving**: atomically move aggregate snapshots + events into a Redis stream
- **Type-safe generics**: no interfaces to implement in your domain types
- **EventHub**: push events to in-process consumers while persisting to Redis

## Core Concepts

- **Timebox**: Root object that owns configuration, lifecycle, and the EventHub.
- **Store**: Redis-backed persistence for events and snapshots; publishes appended events to the hub.
- **Indexer/Index**: Optional append-time projection hook that can update status and label indexes atomically with event persistence.
- **Executor/Aggregator/Command**: Executor loads state (from cache/snapshot/log), runs your command, and persists events raised on the Aggregator with optimistic retries.
- **Appliers**: Pure functions that fold an event into aggregate state. `MakeApplier` lets you work with strongly typed payloads.
- **Handlers/Dispatchers**: Helpers for consuming events from the EventHub without manual JSON decoding.
- **Snapshots**: Created automatically as events grow; also available on demand with `SaveSnapshot`.

## Store Configuration

Store behavior is configured via `StoreConfig` when creating a store.

- `Addr`: Redis/Valkey host:port.
- `Password`: Redis/Valkey password (optional).
- `DB`: Redis/Valkey database index.
- `Prefix`: key prefix for all store data.
- `WorkerCount`: number of background snapshot workers. Set to 0 to disable.
- `MaxQueueSize`: snapshot queue capacity.
- `SaveTimeout`: snapshot persistence timeout.
- `TrimEvents`: when true, snapshots trim events up to the latest snapshot sequence. Default is false.
- `Archiving`: enable `Store.Archive`/`Store.ConsumeArchive` support. Default is false.
- `Indexer`: optional function that derives index mutations from an appended event batch.

## Indexing

`StoreConfig.Indexer` lets you derive index mutations from an appended event
batch. These mutations are persisted atomically with the event append.

`Index` currently supports:

- `Status`: tracks the aggregate's current status and when it entered that
  status. Use `Store.ListAggregatesByStatus` and
  `Store.RemoveAggregateFromStatus` to read or manage that index.
- `Labels`: adds append-only label memberships. Empty values are ignored.

Label indexing maintains two read paths:

- `Store.ListLabelValues(ctx, label)`: returns the unique values seen for a
  label.
- `Store.ListAggregatesByLabel(ctx, label, value)`: returns the aggregate IDs
  indexed under a label/value pair.

Label memberships are append-only. If the same aggregate is later indexed under
another value for the same label, the prior membership remains indexed.

## Archiving

Archiving atomically moves an aggregate's snapshot and full event log into a Redis stream and clears the original keys. It is a one-way operation (no restore API).

Enable it per store with `Archiving`, then call `Store.Archive(ctx, id)`.

To consume archived records, call `Store.ConsumeArchive(ctx, handler)`, which blocks until work is available and processes a single record per call. For a timed wait, call `Store.PollArchive(ctx, timeout, handler)`. Processing is at-least-once, so handlers must be idempotent. Successful handling acknowledges and deletes the stream entry.

Stream, group, and consumer names are derived from the store prefix:

- stream: `<prefix>:archive`
- group: `<prefix>:archive:group`
- consumer: `<prefix>:archive:consumer`

## Examples

- `examples/order.go` is a runnable order lifecycle that shows how appliers, commands, and event consumption fit together.

## Status

Work in progress. Not ready for production use.

---
title: "Production Patterns"
weight: 60
---

# Production Patterns

These patterns show how the core pieces fit into a long-running replicated service.

## One Backend, Several Stores

Open one backend, then create several stores over it when aggregate families need different policies:

- A metadata store for singleton aggregates, with event trimming enabled
- A workflow store with a status/tag indexer and a different snapshot ratio

Stores share persistence while keeping their caching, snapshotting, trimming, and indexing policies separate.

## One Executor per State Model

Create separate executors for each state model. Singleton models use `NewAggregateType`; models with many instances use `NewAggregateID`.

This keeps event dispatch and state reconstruction attached to the domain model that owns them.

## Transactions as Domain Boundaries

Run domain operations inside `Store.Transact`. A parent aggregate can enlist a child aggregate through the same `Transaction`, so their event batches either both commit or neither does.

Keep transaction functions deterministic because conflicts can rerun them. Register scheduling, publication, and other side effects with `OnSuccess` so they begin only after persistence succeeds.

## Committed Event Publication

Configure the Raft backend's `Publisher` before opening it. After a commit is durably applied, the publisher can update live projections or feed an event hub. This separates authoritative persistence from live delivery without putting publication inside command functions.

## Lifecycle

Wait for `Store.WaitReady` with a deadline before serving requests, then close the shared backend during graceful shutdown. Executors manage snapshots automatically as aggregates load.

These are application policies rather than Timebox requirements, but they are a useful starting point for a replicated service.

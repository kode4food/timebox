# Timebox <img src="./docs/logo.png" align="right" height="100"/>

![Build Status](https://github.com/kode4food/timebox/actions/workflows/build.yml/badge.svg) [![Code Coverage](https://qlty.sh/gh/kode4food/projects/timebox/coverage.svg)](https://qlty.sh/gh/kode4food/projects/timebox) [![Maintainability](https://qlty.sh/gh/kode4food/projects/timebox/maintainability.svg)](https://qlty.sh/gh/kode4food/projects/timebox) [![GitHub](https://img.shields.io/github/license/kode4food/timebox)](https://github.com/kode4food/timebox/blob/main/LICENSE)

Timebox is a small, opinionated event sourcing library for Go with append-only event storage, optimistic concurrency, snapshotting, and durable message scheduling. It supports Redis, PostgreSQL, or Raft as its persistence backend.

## Documentation

- [Getting started](https://kode4food.github.io/timebox/docs/getting-started/) explains how to install Timebox and execute a command.
- [Order tutorial](https://kode4food.github.io/timebox/docs/tutorial/) builds an application using aggregates and transactions.
- [Aggregates and events](https://kode4food.github.io/timebox/docs/aggregates/) covers identities, payloads, and appliers.
- [Transactions](https://kode4food.github.io/timebox/docs/transactions/) shows how to commit changes across aggregates.
- [Scheduler](https://kode4food.github.io/timebox/docs/scheduler/) explains how to deliver messages at a durable deadline.
- [Snapshots](https://kode4food.github.io/timebox/docs/snapshots/) explains how to load state without replaying its full history.
- [Indexing](https://kode4food.github.io/timebox/docs/indexing/) covers queries by aggregate status and tags.
- [Archiving](https://kode4food.github.io/timebox/docs/archiving/) shows how to remove finished aggregates from live storage and export their records.
- [Backends](https://kode4food.github.io/timebox/docs/backends/) helps you choose and configure persistence.
- [Production patterns](https://kode4food.github.io/timebox/docs/patterns/) covers the structure of a long-running service.

The [order example](examples/order.go) shows an aggregate lifecycle using Redis.

## Status

Work in progress. Not ready for production use.

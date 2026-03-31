// Package timebox implements an event sourcing toolkit with pluggable
// persistence backends such as memory, Redis/Valkey, PostgreSQL, and Raft. It
// couples an append-only event log, snapshots, optimistic concurrency, and
// append-time indexing into a library that can be embedded into services
//
// Typical usage looks like:
//   - Open backend persistence and create a Store from it
//   - Define Appliers that fold events into your aggregate state
//   - Optionally define an Indexer to project current status or label indexes
//   - Use an Executor to run Commands that raise events on an Aggregator
//   - Save snapshots explicitly or let the Executor refresh them while loading
//   - Query the Store directly for events and aggregate state
//
// The examples/ directory contains a runnable order workflow that exercises
// the API in a small domain
package timebox

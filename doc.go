// Package timebox implements an event sourcing toolkit backed by Redis or
// Valkey. It couples an append-only event log, snapshots, optimistic
// concurrency, append-time indexing, and an in-process event hub into a single
// library that can be embedded into services.
//
// Typical usage looks like:
//   - Create a Timebox with configuration
//   - Open a Store backed by Redis
//   - Define Appliers that fold events into your aggregate state
//   - Optionally define an Indexer to project current status or label indexes
//   - Use an Executor to run Commands that raise events on an Aggregator
//   - Consume events from the EventHub or by querying the Store directly
//
// The examples/ directory contains a runnable order workflow that exercises
// the API in a small domain.
package timebox

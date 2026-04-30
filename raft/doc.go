// Package raft implements Timebox persistence semantics using Raft for the
// replicated commit path, a durable write-ahead log, and a local materialized
// read store
//
// The core correctness boundary remains the Timebox append contract:
// aggregate-local optimistic concurrency, atomic event-batch append, and
// aligned derived index updates. Timebox snapshots are stored through the
// replicated state machine as accelerative state, not as the authoritative
// source of truth
//
// Archive lifecycle support is replicated through Raft and stored in the local
// materialized state
package raft

// Package raft implements Timebox persistence semantics using etcd Raft as
// the replicated commit path, the etcd WAL as the durable log, and bbolt as
// the local materialized read store
//
// The core correctness boundary remains the Timebox append contract:
// aggregate-local optimistic concurrency, atomic event-batch append, and
// aligned derived index updates. Timebox snapshots are stored through the
// replicated state machine as accelerative state, not as the authoritative
// source of truth
//
// Archive lifecycle support is not implemented by this backend. Store archive
// calls therefore return timebox.ErrArchivingDisabled
package raft

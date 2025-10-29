# Timebox

An event sourcing library for Go with built-in distributed coordination.

## Features

- **Event Sourcing**: Complete event sourcing implementation with immutable event log
- **Zero Boilerplate**: No interfaces to implement, no sequence tracking in your state
- **Automatic Sequence Management**: Library handles all sequence tracking internally
- **Optimistic Concurrency**: Sequence-based versioning with automatic retry
- **Snapshot Optimization**: Automatic snapshot management with background workers
- **Distributed Coordination**: Multi-instance support via shared Redis/Valkey backend
- **Type-Safe Generics**: Works with any Go type - no interface requirements
- **LRU Caching**: Per-instance caching with entry-level locking
- **EventHub**: Event distribution using pull-based consumers

# Status

Work in progress. Not ready for production use.

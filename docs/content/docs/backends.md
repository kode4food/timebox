---
title: "Backends"
weight: 40
---

# Backends

All backends implement the same append, snapshot, query, and optimistic-concurrency contract. Choose one backend for storage and create one or more stores over it.

## Memory

```go
backend := memory.Open()
store, err := backend.NewStore()
```

Use memory for tests and single-process ephemeral state. It supports indexing and archiving.

## PostgreSQL

```go
backend, err := postgres.Open(postgres.Config{
    URL:      "postgres://localhost:5432/app?sslmode=disable",
    Prefix:   "timebox",
    MaxConns: 32,
})
if err != nil {
    return err
}
defer backend.Close()

store, err := backend.NewStore()
```

The backend creates and owns its schema. `Prefix` is the PostgreSQL schema name. PostgreSQL supports events, snapshots, status, tags, multi-aggregate transactions, and archiving.

## Redis and Valkey

```go
backend, err := redis.Open(redis.Config{
    Addr:     "127.0.0.1:6379",
    Password: os.Getenv("REDIS_PASSWORD"),
    Prefix:   "timebox",
    DB:       0,
})
if err != nil {
    return err
}
defer backend.Close()

store, err := backend.NewStore()
```

On Redis Cluster, set `Shard`. It adds a hash tag that places every Timebox key in one cluster slot, as atomic multi-key operations require.

## Raft

```go
cfg := raft.Config{
    LocalID: "node-1",
    Address: "127.0.0.1:9001",
    DataDir: "/var/lib/myapp/timebox",
    Servers: []raft.Server{
        {ID: "node-1", Address: "127.0.0.1:9001"},
    },
}

backend, err := raft.Open(cfg)
if err != nil {
    return err
}
defer backend.Close()

store, err := backend.NewStore()
if err != nil {
    return err
}

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := store.WaitReady(ctx); err != nil {
    return err
}
```

The Raft backend replicates writes across its voters and serves reads locally. Configure the same voter set on every node with a unique `LocalID`, `Address`, and `DataDir` per node.

`Publisher` receives committed events after they are durably applied:

```go
cfg.Publisher = func(events ...*timebox.Event) {
    committed.Publish(events...)
}
```

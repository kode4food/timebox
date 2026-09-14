---
title: "Getting Started"
weight: 10
---

# Getting Started

## Requirements

- Go 1.27 or later
- A supported persistence service when using PostgreSQL or Redis/Valkey

## Install

```sh
go get github.com/kode4food/timebox@v0.1.0
```

Backend packages are part of the same module:

```go
import (
    "github.com/kode4food/timebox"
    "github.com/kode4food/timebox/memory"
)
```

## Define an Aggregate

An aggregate needs a state type, an empty-state constructor, event types, event payloads, and appliers. Appliers rebuild state from stored events and must be deterministic.

```go
type Account struct {
    Balance int
}

type Deposited struct {
    Amount int `json:"amount"`
}

const AccountDeposited timebox.EventType = "account.deposited"

func newAccount() Account {
    return Account{}
}

var accountAppliers = timebox.Appliers[Account]{
    AccountDeposited: timebox.MakeApplier(
        func(account Account, _ *timebox.Event, data Deposited) Account {
            account.Balance += data.Amount
            return account
        },
    ),
}
```

`MakeApplier` decodes the payload and passes both the event metadata and typed data to the function.

## Open a Store

The memory backend is useful for tests and single-process tools:

```go
backend := memory.Open()
defer backend.Close()

store, err := backend.NewStore()
if err != nil {
    return err
}
```

## Execute a Command

Create an executor for the aggregate state and address each aggregate with both a type and key:

```go
accounts := store.Executor(newAccount, accountAppliers)
accountID := timebox.NewAggregateID("account", "ACC-123")

account, err := accounts.Exec(accountID,
    func(account Account, ag *timebox.Aggregator[Account]) error {
        if account.Balance < 0 {
            return errors.New("invalid balance")
        }
        return ag.Raise(AccountDeposited, Deposited{Amount: 50})
    },
)
```

The command receives the current state. `Raise` serializes an event, applies it immediately, and stages it for an atomic append. If another writer wins first, Timebox reloads the conflicting events and reruns the command up to `MaxRetries`.

Commands can raise several events. Returning an error discards all events raised by that execution.

## Read State and Events

`Get` reconstructs the latest state without raising an event:

```go
account, err := accounts.Get(accountID)
```

Raw events remain available when an event consumer or audit path needs them:

```go
events, err := store.GetEvents(accountID, 0)
```

For a complete build-along example, continue with [Tutorial: Place an Order]({{< relref "/docs/tutorial" >}}). A larger order lifecycle is also available in [`examples/order.go`](https://github.com/kode4food/timebox/blob/main/examples/order.go).

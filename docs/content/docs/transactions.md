---
title: "Transactions"
weight: 30
---

# Transactions

`Executor.Exec` is a single-aggregate transaction. Use `Store.Transact` when one operation must update several aggregates atomically:

```go
err := store.Transact(func(tx *timebox.Transaction) error {
    if _, err := tx.Exec(accounts, sourceID,
        func(source Account, ag *timebox.Aggregator[Account]) error {
            if source.Balance < amount {
                return ErrInsufficientFunds
            }
            return ag.Raise(MoneyWithdrawn, Money{Amount: amount})
        },
    ); err != nil {
        return err
    }

    _, err := tx.Exec(accounts, destinationID,
        func(_ Account, ag *timebox.Aggregator[Account]) error {
            return ag.Raise(MoneyDeposited, Money{Amount: amount})
        },
    )
    return err
})
```

Every backend appends all enlisted aggregate batches or none of them. Each aggregate has its own optimistic sequence check. On a conflict, Timebox refreshes state and reruns the entire transaction function.

## Rules

- Executors passed to a transaction must belong to its store
- Reusing an aggregate in one transaction continues the same in-memory `Aggregator`
- Joining one aggregate through executors with different state types returns `ErrAggregateTypeConflict`
- Values returned by `Transaction.Exec` are provisional until `Transact` succeeds
- Command and transaction functions may run more than once, so external side effects belong in `OnSuccess`

## Nested Domain Operations

An aggregate command can reach its enclosing transaction through `Aggregator.Transaction()` and enlist another aggregate. For example, a parent workflow can start a child workflow in the same commit as its own update.

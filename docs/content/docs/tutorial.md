---
title: "Tutorial: Place an Order"
weight: 15
---

# Tutorial: Place an Order

This tutorial builds a small order service from an empty Go module. It receives inventory, atomically reserves stock while placing an order, confirms and ships the order, and queries the resulting event history, status index, and customer tags.

The finished program uses the in-memory backend so it runs without infrastructure. The final step shows how to replace memory with durable storage without changing the domain code.

## 1. Create the Project

```sh
mkdir timebox-orders
cd timebox-orders
go mod init example.com/timebox-orders
go get github.com/kode4food/timebox@v0.1.0
```

Create `main.go` with the package and imports:

```go
package main

import (
    "errors"
    "fmt"

    "github.com/kode4food/timebox"
    "github.com/kode4food/timebox/memory"
)
```

## 2. Model the Current State

The service has two aggregate types. A product tracks stock and an order tracks one customer's request.

```go
type (
    // Type aliases for convenience

    ProductAggregator = timebox.Aggregator[ProductState]
    ProductAppliers   = timebox.Appliers[ProductState]
    ProductExecutor   = timebox.Executor[ProductState]
    OrderAggregator   = timebox.Aggregator[OrderState]
    OrderAppliers     = timebox.Appliers[OrderState]
    OrderExecutor     = timebox.Executor[OrderState]

    // Domain types

    ProductState struct {
        SKU       string
        Available int
        Reserved  int
        Shipped   int
    }

    OrderState struct {
        CustomerID     string
        SKU            string
        Status         string
        TrackingNumber string
        Quantity       int
    }
)
```

These values are projections, not stored records. Timebox reconstructs them from their event histories.

## 3. Define the Events

Event names and payloads are the durable history of the application. Name them for facts that happened, not commands someone attempted.

```go
type (
    // Event data types

    StockChangedData struct {
        SKU      string `json:"sku"`
        Quantity int    `json:"quantity"`
    }

    OrderPlacedData struct {
        CustomerID string `json:"customer_id"`
        SKU        string `json:"sku"`
        Quantity   int    `json:"quantity"`
    }

    OrderShippedData struct {
        TrackingNumber string `json:"tracking_number"`
    }
)

const (
    // Event types

    StockReceived  timebox.EventType = "stock.received"
    StockReserved  timebox.EventType = "stock.reserved"
    StockShipped   timebox.EventType = "stock.shipped"
    OrderPlaced    timebox.EventType = "order.placed"
    OrderConfirmed timebox.EventType = "order.confirmed"
    OrderShipped   timebox.EventType = "order.shipped"

    // Order statuses

    StatusPlaced    = "placed"
    StatusConfirmed = "confirmed"
    StatusShipped   = "shipped"
)
```

Payloads are JSON encoded when raised. Changing an existing payload later means preserving the ability to read events already stored with the old shape.

## 4. Rebuild State with Appliers

Constructors provide empty state. Appliers turn one state plus one event into the next state.

```go
func NewProductState() ProductState {
    return ProductState{}
}

func NewOrderState() OrderState {
    return OrderState{}
}

func applyStockReceived(
    state ProductState, _ *timebox.Event, data StockChangedData,
) ProductState {
    state.SKU = data.SKU
    state.Available += data.Quantity
    return state
}

func applyStockReserved(
    state ProductState, _ *timebox.Event, data StockChangedData,
) ProductState {
    state.Available -= data.Quantity
    state.Reserved += data.Quantity
    return state
}

func applyStockShipped(
    state ProductState, _ *timebox.Event, data StockChangedData,
) ProductState {
    state.Reserved -= data.Quantity
    state.Shipped += data.Quantity
    return state
}

func applyOrderPlaced(
    state OrderState, _ *timebox.Event, data OrderPlacedData,
) OrderState {
    state.CustomerID = data.CustomerID
    state.SKU = data.SKU
    state.Quantity = data.Quantity
    state.Status = StatusPlaced
    return state
}

func applyOrderConfirmed(
    state OrderState, _ *timebox.Event, _ struct{},
) OrderState {
    state.Status = StatusConfirmed
    return state
}

func applyOrderShipped(
    state OrderState, _ *timebox.Event, data OrderShippedData,
) OrderState {
    state.Status = StatusShipped
    state.TrackingNumber = data.TrackingNumber
    return state
}
```

Create one executor for each state model and register its appliers under their event types. `MakeApplier` handles typed payload decoding.

```go
func createProductExecutor(store *timebox.Store) *ProductExecutor {
    return store.Executor(NewProductState, ProductAppliers{
        StockReceived: timebox.MakeApplier(applyStockReceived),
        StockReserved: timebox.MakeApplier(applyStockReserved),
        StockShipped:  timebox.MakeApplier(applyStockShipped),
    })
}

func createOrderExecutor(store *timebox.Store) *OrderExecutor {
    return store.Executor(NewOrderState, OrderAppliers{
        OrderPlaced:    timebox.MakeApplier(applyOrderPlaced),
        OrderConfirmed: timebox.MakeApplier(applyOrderConfirmed),
        OrderShipped:   timebox.MakeApplier(applyOrderShipped),
    })
}
```

Appliers must be deterministic and free of side effects because they run during replay and immediately after an event is raised.

## 5. Add Status and Tag Indexes

Indexes are derived in the same atomic append as their events. This indexer makes orders queryable by lifecycle status, customer, and SKU without replaying every order.

```go
func orderIndexer(events []*timebox.Event) []*timebox.Index {
    var indexes []*timebox.Index
    for _, event := range events {
        var status string
        var tags map[string]bool
        switch event.Type {
        case OrderPlaced:
            status = StatusPlaced
            data, _ := event.GetValue[OrderPlacedData]()
            tags = map[string]bool{
                "customer:" + data.CustomerID: true,
                "sku:" + data.SKU:             true,
            }
        case OrderConfirmed:
            status = StatusConfirmed
        case OrderShipped:
            status = StatusShipped
        default:
            continue
        }
        indexes = append(indexes, &timebox.Index{
            Status: &status,
            Tags:   tags,
        })
    }
    return indexes
}
```

The same store also receives inventory events. The indexer ignores those event types. Tags added when an order is placed remain attached as its status changes.

## 6. Receive Stock

An executor loads the product, runs the command, and appends the event only if the expected sequence still matches.

```go
func receiveStock(
    products *ProductExecutor, id timebox.AggregateID, quantity int,
) error {
    _, err := products.Exec(id,
        func(
            _ ProductState,
            ag *ProductAggregator,
        ) error {
            return ag.Raise(StockReceived, StockChangedData{
                SKU:      string(id.Key),
                Quantity: quantity,
            })
        },
    )
    return err
}
```

`Raise` serializes the event and applies it to the current state immediately. Returning an error from the command would discard everything raised during that execution.

## 7. Reserve Stock and Place the Order Atomically

Stock must not be reserved unless the order is also created. Use one transaction across both aggregates.

```go
var (
    errInsufficientStock = errors.New("insufficient stock")
    errOrderExists       = errors.New("order already exists")
)

type placeOrderArgs struct {
    store             *timebox.Store
    products          *ProductExecutor
    orders            *OrderExecutor
    productID, orderID timebox.AggregateID
    customerID        string
    quantity          int
}

func placeOrder(args placeOrderArgs) error {
    return args.store.Transact(func(tx *timebox.Transaction) error {
        _, err := tx.Exec(args.products, args.productID,
            func(
                product ProductState,
                ag *ProductAggregator,
            ) error {
                if product.Available < args.quantity {
                    return errInsufficientStock
                }
                return ag.Raise(StockReserved, StockChangedData{
                    SKU:      product.SKU,
                    Quantity: args.quantity,
                })
            },
        )
        if err != nil {
            return err
        }

        _, err = tx.Exec(args.orders, args.orderID,
            func(
                order OrderState,
                ag *OrderAggregator,
            ) error {
                if order.Status != "" {
                    return errOrderExists
                }
                return ag.Raise(OrderPlaced, OrderPlacedData{
                    CustomerID: args.customerID,
                    SKU:        string(args.productID.Key),
                    Quantity:   args.quantity,
                })
            },
        )
        return err
    })
}
```

Every backend checks both aggregate sequences before appending either batch. A conflict reloads state and reruns the transaction, so transaction functions must not perform external side effects. Use `Aggregator.OnSuccess` for work that should begin only after commit.

## 8. Confirm the Order

The current projection lets a command enforce domain rules before raising the next event.

```go
func confirmOrder(
    orders *OrderExecutor, id timebox.AggregateID,
) error {
    _, err := orders.Exec(id,
        func(order OrderState, ag *OrderAggregator) error {
            if order.Status != StatusPlaced {
                return fmt.Errorf(
                    "cannot confirm order in %q state", order.Status,
                )
            }
            return ag.Raise(OrderConfirmed, struct{}{})
        },
    )
    return err
}
```

## 9. Ship the Order and Inventory Atomically

Shipping changes both aggregates again: the order gains a tracking number while reserved inventory becomes shipped inventory. Keeping those changes in one transaction prevents an order from appearing shipped while its stock remains reserved.

```go
type shipOrderArgs struct {
    store              *timebox.Store
    products           *ProductExecutor
    orders             *OrderExecutor
    productID, orderID timebox.AggregateID
    trackingNumber     string
}

func shipOrder(args shipOrderArgs) error {
    return args.store.Transact(func(tx *timebox.Transaction) error {
        order, err := tx.Exec(args.orders, args.orderID,
            func(
                order OrderState,
                ag *OrderAggregator,
            ) error {
                if order.Status != StatusConfirmed {
                    return fmt.Errorf(
                        "cannot ship order in %q state", order.Status,
                    )
                }
                return ag.Raise(OrderShipped, OrderShippedData{
                    TrackingNumber: args.trackingNumber,
                })
            },
        )
        if err != nil {
            return err
        }

        _, err = tx.Exec(args.products, args.productID,
            func(
                product ProductState,
                ag *ProductAggregator,
            ) error {
                if product.Reserved < order.Quantity {
                    return errInsufficientStock
                }
                return ag.Raise(StockShipped, StockChangedData{
                    SKU:      product.SKU,
                    Quantity: order.Quantity,
                })
            },
        )
        return err
    })
}
```

The state returned by the first `tx.Exec` includes the newly raised shipping event, so the inventory update can use the order's quantity. Neither aggregate commits unless both commands succeed.

## 10. Wire and Run the Service

Add `main` and `run`. The memory backend provides the complete Timebox contract without an external service.

```go
func main() {
    if err := run(); err != nil {
        panic(err)
    }
}

func run() error {
    backend := memory.Open()
    defer func() { _ = backend.Close() }()

    store, err := backend.NewStore(timebox.Config{Indexer: orderIndexer})
    if err != nil {
        return err
    }

    products := createProductExecutor(store)
    orders := createOrderExecutor(store)
    productID := timebox.NewAggregateID("product", "SKU-RED-MUG")
    orderID := timebox.NewAggregateID("order", "ORD-1001")

    if err := receiveStock(products, productID, 10); err != nil {
        return err
    }
    if err := placeOrder(placeOrderArgs{
        store:      store,
        products:   products,
        orders:     orders,
        productID:  productID,
        orderID:    orderID,
        customerID: "CUS-42",
        quantity:   3,
    }); err != nil {
        return err
    }
    if err := confirmOrder(orders, orderID); err != nil {
        return err
    }
    if err := shipOrder(shipOrderArgs{
        store:          store,
        products:       products,
        orders:         orders,
        productID:      productID,
        orderID:        orderID,
        trackingNumber: "TRACK-9000",
    }); err != nil {
        return err
    }
```

Read the resulting projections and inspect the history, status index, and customer tag:

```go
    product, err := products.Get(productID)
    if err != nil {
        return err
    }
    order, err := orders.Get(orderID)
    if err != nil {
        return err
    }
    events, err := store.GetEvents(orderID, 0)
    if err != nil {
        return err
    }
    shipped, err := store.ListAggregatesByStatus("shipped")
    if err != nil {
        return err
    }
    customerOrders, err := store.ListAggregatesByTag("customer:CUS-42")
    if err != nil {
        return err
    }

    fmt.Printf("order: %s x%d (%s, %s)\n", order.SKU, order.Quantity,
        order.Status, order.TrackingNumber)
    fmt.Printf("stock: %d available, %d reserved, %d shipped\n",
        product.Available, product.Reserved, product.Shipped)
    fmt.Printf("history: %d events; shipped index: %d; customer index: %d\n",
        len(events), len(shipped), len(customerOrders))
    return nil
}
```

Run it:

```sh
go run .
```

The result is:

```text
order: SKU-RED-MUG x3 (shipped, TRACK-9000)
stock: 7 available, 0 reserved, 3 shipped
history: 3 events; shipped index: 1; customer index: 1
```

The complete runnable source is in [`examples/tutorial/main.go`](https://github.com/kode4food/timebox/blob/main/examples/tutorial/main.go).

## 11. Use Durable Storage

Only backend construction changes when the service needs persistence. For PostgreSQL:

```go
backend, err := postgres.Open(postgres.Config{
    URL:    os.Getenv("DATABASE_URL"),
    Prefix: "orders",
})
if err != nil {
    return err
}
defer backend.Close()

store, err := backend.NewStore(timebox.Config{Indexer: orderIndexer})
```

The aggregate models, appliers, commands, transactions, snapshots, and queries stay the same. See [Backends]({{< relref "/docs/backends" >}}) for Redis/Valkey and Raft configuration.

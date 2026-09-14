package main

import (
	"errors"
	"fmt"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/memory"
)

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

var (
	errInsufficientStock = errors.New("insufficient stock")
	errOrderExists       = errors.New("order already exists")
)

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

func NewProductState() ProductState {
	return ProductState{}
}

func NewOrderState() OrderState {
	return OrderState{}
}

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

func receiveStock(
	products *ProductExecutor, id timebox.AggregateID, quantity int,
) error {
	_, err := products.Exec(id,
		func(_ ProductState, ag *ProductAggregator) error {
			return ag.Raise(StockReceived, StockChangedData{
				SKU:      string(id.Key),
				Quantity: quantity,
			})
		},
	)
	return err
}

type placeOrderArgs struct {
	store              *timebox.Store
	products           *ProductExecutor
	orders             *OrderExecutor
	productID, orderID timebox.AggregateID
	customerID         string
	quantity           int
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

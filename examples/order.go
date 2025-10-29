package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"

	"github.com/kode4food/timebox"
)

type (
	// Type aliases for convenience
	OrderAggregator = timebox.Aggregator[*OrderState]
	OrderApplier    = timebox.Applier[*OrderState]
	OrderAppliers   = timebox.Appliers[*OrderState]
	OrderExecutor   = timebox.Executor[*OrderState]

	// Domain types
	Address struct {
		Street  string `json:"street"`
		City    string `json:"city"`
		State   string `json:"state"`
		ZipCode string `json:"zip_code"`
		Country string `json:"country"`
	}

	OrderItem struct {
		ProductID string  `json:"product_id"`
		Name      string  `json:"name"`
		Quantity  int     `json:"quantity"`
		Price     float64 `json:"price"`
	}

	OrderState struct {
		CustomerName    string               `json:"customer_name"`
		CustomerEmail   string               `json:"customer_email"`
		Items           map[string]OrderItem `json:"items"`
		ShippingAddress *Address             `json:"shipping_address"`
		BillingAddress  *Address             `json:"billing_address"`
		Status          string               `json:"status"`
		Total           float64              `json:"total"`
	}

	// Event data types
	OrderCreatedData struct {
		CustomerName  string `json:"customer_name"`
		CustomerEmail string `json:"customer_email"`
	}

	ItemAddedData struct {
		ProductID string  `json:"product_id"`
		Name      string  `json:"name"`
		Quantity  int     `json:"quantity"`
		Price     float64 `json:"price"`
	}

	ItemRemovedData struct {
		ProductID string `json:"product_id"`
	}

	AddressChangedData struct {
		Address Address `json:"address"`
	}
)

const (
	// Event types
	OrderCreated         timebox.EventType = "order.created"
	OrderItemAdded       timebox.EventType = "order.item_added"
	OrderItemRemoved     timebox.EventType = "order.item_removed"
	OrderShippingChanged timebox.EventType = "order.shipping_changed"
	OrderBillingChanged  timebox.EventType = "order.billing_changed"
	OrderConfirmed       timebox.EventType = "order.confirmed"
	OrderShipped         timebox.EventType = "order.shipped"
	OrderDelivered       timebox.EventType = "order.delivered"

	// Order statuses
	StatusDraft     = "draft"
	StatusCreated   = "created"
	StatusConfirmed = "confirmed"
	StatusShipped   = "shipped"
	StatusDelivered = "delivered"
)

// Appliers implement business logic for order events
var appliers = OrderAppliers{
	OrderCreated:         orderCreated,
	OrderItemAdded:       orderItemAdded,
	OrderItemRemoved:     orderItemRemoved,
	OrderShippingChanged: orderShippingChanged,
	OrderBillingChanged:  orderBillingChanged,
	OrderConfirmed:       orderConfirmed,
	OrderShipped:         orderShipped,
	OrderDelivered:       orderDelivered,
}

type orderExample struct {
	ctx      context.Context
	executor *OrderExecutor
	orderID  timebox.AggregateID
}

func main() {
	ex := &orderExample{
		ctx:      context.Background(),
		executor: setupTimebox(),
		orderID:  timebox.NewAggregateID("order", "ORD-12345"),
	}
	defer ex.executor.GetStore().Close()

	ex.createOrder()
	ex.addShippingAddress()
	ex.confirmOrder()
	ex.shipOrder()
	ex.deliverOrder()

	fmt.Println("\nOrder lifecycle complete!")
}

func setupTimebox() *OrderExecutor {
	cfg := timebox.DefaultConfig()
	cfg.Store.Prefix = "example"
	cfg.CacheSize = 4096
	cfg.MaxRetries = 10
	cfg.EnableSnapshotWorker = true

	tb, err := timebox.NewTimebox(cfg)
	if err != nil {
		log.Fatal(err)
	}

	return timebox.NewExecutor(tb, appliers, NewOrderState)
}

func (ex *orderExample) createOrder() {
	fmt.Println("Creating order...")
	state, err := ex.executor.Exec(ex.ctx, ex.orderID,
		func(s *OrderState, ag *OrderAggregator) error {
			// Create order
			data, _ := json.Marshal(OrderCreatedData{
				CustomerName:  "John Doe",
				CustomerEmail: "john@example.com",
			})
			ag.Raise(OrderCreated, data)

			// Add items to order
			item1, _ := json.Marshal(ItemAddedData{
				ProductID: "LAPTOP-PRO",
				Name:      "Professional Laptop",
				Quantity:  1,
				Price:     1299.99,
			})
			ag.Raise(OrderItemAdded, item1)

			item2, _ := json.Marshal(ItemAddedData{
				ProductID: "MOUSE-WIRELESS",
				Name:      "Wireless Mouse",
				Quantity:  2,
				Price:     29.99,
			})
			ag.Raise(OrderItemAdded, item2)

			return nil
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Order created for %s (%s)\n",
		state.CustomerName,
		state.CustomerEmail)

	fmt.Printf("Items: %d, Total: $%.2f\n", len(state.Items), state.Total)
}

func (ex *orderExample) addShippingAddress() {
	fmt.Println("\nAdding shipping address...")
	state, err := ex.executor.Exec(ex.ctx, ex.orderID,
		func(s *OrderState, ag *OrderAggregator) error {
			addr, _ := json.Marshal(AddressChangedData{
				Address: Address{
					Street:  "123 Main St",
					City:    "San Francisco",
					State:   "CA",
					ZipCode: "94102",
					Country: "USA",
				},
			})
			ag.Raise(OrderShippingChanged, addr)
			return nil
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Shipping to: %s, %s, %s %s\n",
		state.ShippingAddress.Street,
		state.ShippingAddress.City,
		state.ShippingAddress.State,
		state.ShippingAddress.ZipCode)
}

func (ex *orderExample) confirmOrder() {
	fmt.Println("\nConfirming order...")
	state, err := ex.executor.Exec(ex.ctx, ex.orderID,
		func(s *OrderState, ag *OrderAggregator) error {
			ag.Raise(OrderConfirmed, json.RawMessage("{}"))
			return nil
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Order status: %s\n", state.Status)
}

func (ex *orderExample) shipOrder() {
	fmt.Println("\nShipping order...")
	state, err := ex.executor.Exec(ex.ctx, ex.orderID,
		func(s *OrderState, ag *OrderAggregator) error {
			ag.Raise(OrderShipped, json.RawMessage("{}"))
			return nil
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Order status: %s\n", state.Status)
}

func (ex *orderExample) deliverOrder() {
	fmt.Println("\nDelivering order...")
	state, err := ex.executor.Exec(ex.ctx, ex.orderID,
		func(s *OrderState, ag *OrderAggregator) error {
			ag.Raise(OrderDelivered, json.RawMessage("{}"))
			return nil
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Order status: %s\n", state.Status)
}

func NewOrderState() *OrderState {
	return &OrderState{
		Items:  make(map[string]OrderItem),
		Status: StatusDraft,
		Total:  0,
	}
}

// Applier functions

func orderCreated(state *OrderState, ev *timebox.Event) *OrderState {
	var data OrderCreatedData
	_ = json.Unmarshal(ev.Data, &data)
	res := *state
	res.CustomerName = data.CustomerName
	res.CustomerEmail = data.CustomerEmail
	res.Status = StatusCreated
	return &res
}

func orderItemAdded(state *OrderState, ev *timebox.Event) *OrderState {
	var data ItemAddedData
	_ = json.Unmarshal(ev.Data, &data)
	res := *state
	res.Items = maps.Clone(state.Items)
	res.Items[data.ProductID] = OrderItem{
		ProductID: data.ProductID,
		Name:      data.Name,
		Quantity:  data.Quantity,
		Price:     data.Price,
	}
	res.Total = calculateTotal(res.Items)
	return &res
}

func orderItemRemoved(state *OrderState, ev *timebox.Event) *OrderState {
	var data ItemRemovedData
	_ = json.Unmarshal(ev.Data, &data)
	res := *state
	res.Items = maps.Clone(state.Items)
	delete(res.Items, data.ProductID)
	res.Total = calculateTotal(res.Items)
	return &res
}

func orderShippingChanged(state *OrderState, ev *timebox.Event) *OrderState {
	var data AddressChangedData
	_ = json.Unmarshal(ev.Data, &data)
	res := *state
	addr := data.Address
	res.ShippingAddress = &addr
	return &res
}

func orderBillingChanged(state *OrderState, ev *timebox.Event) *OrderState {
	var data AddressChangedData
	_ = json.Unmarshal(ev.Data, &data)
	res := *state
	addr := data.Address
	res.BillingAddress = &addr
	return &res
}

func orderConfirmed(state *OrderState, ev *timebox.Event) *OrderState {
	res := *state
	res.Status = StatusConfirmed
	return &res
}

func orderShipped(state *OrderState, ev *timebox.Event) *OrderState {
	res := *state
	res.Status = StatusShipped
	return &res
}

func orderDelivered(state *OrderState, ev *timebox.Event) *OrderState {
	res := *state
	res.Status = StatusDelivered
	return &res
}

func calculateTotal(items map[string]OrderItem) float64 {
	var total float64
	for _, item := range items {
		total += item.Price * float64(item.Quantity)
	}
	return total
}

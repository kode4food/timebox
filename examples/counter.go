package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/kode4food/timebox"
)

type CounterState struct {
	Value int `json:"value"`
}

func NewCounterState() *CounterState {
	return &CounterState{Value: 0}
}

const (
	EventIncremented timebox.EventType = "incremented"
	EventDecremented timebox.EventType = "decremented"
	EventReset       timebox.EventType = "reset"
)

var appliers = map[timebox.EventType]timebox.Applier[*CounterState]{
	EventIncremented: func(state *CounterState, ev *timebox.Event) *CounterState {
		var delta int
		if err := json.Unmarshal(ev.Data, &delta); err != nil {
			return state
		}
		return &CounterState{Value: state.Value + delta}
	},
	EventDecremented: func(state *CounterState, ev *timebox.Event) *CounterState {
		var delta int
		if err := json.Unmarshal(ev.Data, &delta); err != nil {
			return state
		}
		return &CounterState{Value: state.Value - delta}
	},
	EventReset: func(state *CounterState, ev *timebox.Event) *CounterState {
		return &CounterState{Value: 0}
	},
}

func main() {
	// Create a Timebox with configuration
	cfg := timebox.DefaultConfig()
	cfg.Store.Prefix = "example"
	cfg.CacheSize = 4096
	cfg.MaxRetries = 10
	cfg.EnableSnapshotWorker = true // Snapshot worker will handle automatic snapshots

	tb, err := timebox.NewTimebox(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer tb.Close()

	// Create an executor using the Timebox
	executor := timebox.NewExecutor(tb, appliers, NewCounterState)

	ctx := context.Background()
	counterID := timebox.NewAggregateID("counter", "demo")

	state, err := executor.Exec(ctx, counterID, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		data, _ := json.Marshal(5)
		ag.Raise(EventIncremented, data)

		data, _ = json.Marshal(3)
		ag.Raise(EventIncremented, data)

		data, _ = json.Marshal(2)
		ag.Raise(EventDecremented, data)

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Final counter value: %d\n", state.Value)

	state, err = executor.Exec(ctx, counterID, func(s *CounterState, ag *timebox.Aggregator[*CounterState]) error {
		ag.Raise(EventReset, json.RawMessage("{}"))
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Counter after reset: %d\n", state.Value)
}

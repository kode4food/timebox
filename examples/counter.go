package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/kode4food/timebox"
)

type CounterState struct {
	Value        int   `json:"value"`
	NextSequence int64 `json:"next_sequence"`
}

func (s *CounterState) GetNextSequence() int64 {
	return s.NextSequence
}

func NewCounterState() *CounterState {
	return &CounterState{Value: 0, NextSequence: 0}
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
		return &CounterState{
			Value:        state.Value + delta,
			NextSequence: ev.Sequence + 1,
		}
	},
	EventDecremented: func(state *CounterState, ev *timebox.Event) *CounterState {
		var delta int
		if err := json.Unmarshal(ev.Data, &delta); err != nil {
			return state
		}
		return &CounterState{
			Value:        state.Value - delta,
			NextSequence: ev.Sequence + 1,
		}
	},
	EventReset: func(state *CounterState, ev *timebox.Event) *CounterState {
		return &CounterState{
			Value:        0,
			NextSequence: ev.Sequence + 1,
		}
	},
}

func main() {
	hub := timebox.NewEventHub()

	cfg := timebox.DefaultStoreConfig()
	cfg.Prefix = "example"

	store, err := timebox.NewStore(hub, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	executor := timebox.NewExecutor(
		store,
		appliers,
		NewCounterState,
		4096,
	)

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

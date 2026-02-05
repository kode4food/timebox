package timebox

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/kode4food/caravan"
)

type (
	// Timebox owns shared resources (configuration, context, and event hub)
	// and is responsible for creating Stores that publish events
	Timebox struct {
		config Config
		hub    *EventHub
		ctx    context.Context
		cancel context.CancelFunc
	}

	// Event represents a single immutable event in the log, including its
	// sequence, timestamp, and serialized payload
	Event struct {
		Timestamp   time.Time       `json:"timestamp"`
		Sequence    int64           `json:"sequence"`
		Type        EventType       `json:"type"`
		AggregateID AggregateID     `json:"aggregate_id"`
		Data        json.RawMessage `json:"data"`

		mu    sync.RWMutex
		value any
	}

	// EventType is the string identifier associated with an Event
	EventType string
)

// NewTimebox creates a new Timebox instance with the given configuration and a
// fresh event hub. A Timebox owns the background context used by child Stores
func NewTimebox(cfg Config) (*Timebox, error) {
	ctx, cancel := context.WithCancel(context.Background())
	hub := caravan.NewTopic[*Event]()

	tb := &Timebox{
		config: cfg,
		hub:    NewEventHub(hub),
		ctx:    ctx,
		cancel: cancel,
	}

	return tb, nil
}

// GetHub returns the EventHub instance
func (tb *Timebox) GetHub() *EventHub {
	return tb.hub
}

// Context returns the Timebox's context for cancellation
func (tb *Timebox) Context() context.Context {
	return tb.ctx
}

// Close gracefully shuts down the Timebox
func (tb *Timebox) Close() error {
	tb.cancel()
	return nil
}

// getValue unmarshals the event data into the specified type. It uses a cache
// to avoid repeated unmarshaling from raw JSON bytes. This is safe for
// concurrent access and intended for use by MakeApplier
func (e *Event) getValue(target any) error {
	e.mu.RLock()
	if e.value != nil {
		e.mu.RUnlock()
		data, err := json.Marshal(e.value)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, target)
	}
	e.mu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.value != nil {
		data, err := json.Marshal(e.value)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, target)
	}

	var cache interface{}
	if err := json.Unmarshal(e.Data, &cache); err != nil {
		return err
	}
	e.value = cache

	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

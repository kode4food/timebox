package timebox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/topic"
)

type (
	Timebox struct {
		config Config
		store  *Store
		hub    EventHub
		ctx    context.Context
		cancel context.CancelFunc
	}

	Event struct {
		Timestamp   time.Time       `json:"timestamp"`
		ID          ID              `json:"id"`
		Type        EventType       `json:"type"`
		AggregateID AggregateID     `json:"aggregate_id"`
		Data        json.RawMessage `json:"data"`
	}

	EventHub  topic.Topic[*Event]
	EventType string
)

// NewTimebox creates a new Timebox instance with the given configuration
func NewTimebox(cfg Config) (*Timebox, error) {
	ctx, cancel := context.WithCancel(context.Background())
	hub := caravan.NewTopic[*Event]()

	tb := &Timebox{
		config: cfg,
		hub:    hub,
		ctx:    ctx,
		cancel: cancel,
	}

	store, err := newStore(tb)
	if err != nil {
		cancel()
		return nil, err
	}

	tb.store = store
	return tb, nil
}

// GetStore returns the underlying Store instance.
func (tb *Timebox) GetStore() *Store {
	return tb.store
}

// GetHub returns the EventHub instance.
func (tb *Timebox) GetHub() EventHub {
	return tb.hub
}

// Context returns the Timebox's context for cancellation.
func (tb *Timebox) Context() context.Context {
	return tb.ctx
}

// Close gracefully shuts down the Timebox
func (tb *Timebox) Close() error {
	tb.cancel()
	return tb.store.Close()
}

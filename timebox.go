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
		hub    EventHub
		ctx    context.Context
		cancel context.CancelFunc
	}

	Event struct {
		Timestamp   time.Time       `json:"timestamp"`
		Sequence    int64           `json:"sequence"`
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

	return tb, nil
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
	return nil
}

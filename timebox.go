package timebox

import (
	"context"

	"github.com/kode4food/caravan"
)

type Timebox struct {
	config Config
	store  *Store
	hub    EventHub
	ctx    context.Context
	cancel context.CancelFunc
}

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

package timebox

import (
	"context"
)

type Timebox struct {
	config Config
	store  *Store
	hub    EventHub
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTimebox creates a new Timebox instance with the given configuration.
// It initializes the event hub and store with context management for graceful shutdown.
func NewTimebox(config Config) (*Timebox, error) {
	ctx, cancel := context.WithCancel(context.Background())

	hub := NewEventHub()
	store, err := newStore(ctx, hub, config.Store, config.EnableSnapshotWorker)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Timebox{
		config: config,
		store:  store,
		hub:    hub,
		ctx:    ctx,
		cancel: cancel,
	}, nil
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

// Close gracefully shuts down the Timebox, stopping the snapshot worker
// and closing all connections.
func (tb *Timebox) Close() error {
	tb.cancel()
	return tb.store.Close()
}

package timebox

import (
	"context"
	"errors"
	"log/slog"
)

type (
	// Executor orchestrates loading aggregate state, executing commands, and
	// persisting resulting events with optimistic retries
	Executor[T any] struct {
		store      *Store
		appliers   Appliers[T]
		construct  constructor[T]
		cache      *lruCache[*projection[T]]
		maxRetries int
	}

	// Command is user code that inspects state and raises events on an
	// Aggregator. Returning an error aborts the operation
	Command[T any] func(T, *Aggregator[T]) error

	projection[T any] struct {
		state   T
		nextSeq int64
	}
)

var (
	// ErrMaxRetriesExceeded indicates optimistic concurrency retries were
	// exhausted while attempting to persist events
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

// NewExecutor constructs an Executor bound to a Store with the given
// appliers and state constructor
func NewExecutor[T any](
	store *Store, cons constructor[T], apps Appliers[T],
) *Executor[T] {
	return &Executor[T]{
		store:      store,
		appliers:   apps,
		construct:  cons,
		cache:      newLRUCache[*projection[T]](store.tb.config.CacheSize),
		maxRetries: store.tb.config.MaxRetries,
	}
}

// GetStore exposes the Store used by the Executor
func (e *Executor[T]) GetStore() *Store {
	return e.store
}

// AppliesEvent reports whether the executor has an applier for the event type
func (e *Executor[T]) AppliesEvent(ev *Event) bool {
	_, ok := e.appliers[ev.Type]
	return ok
}

// Exec loads the aggregate state, executes the command, and persists raised
// events. It retries on version conflicts up to MaxRetries
func (e *Executor[T]) Exec(
	ctx context.Context, id AggregateID, cmd Command[T],
) (T, error) {
	var zero T
	for range e.maxRetries {
		proj, err := e.loadSnapshot(ctx, id)
		if err != nil {
			return zero, err
		}

		ag := newAggregator(id, e.appliers, proj.state, proj.nextSeq)
		if err := cmd(ag.Value(), ag); err != nil {
			return zero, err
		}

		count, err := ag.Flush(func(expectedSeq int64, evs []*Event) error {
			return e.store.AppendEvents(ctx, id, expectedSeq, evs)
		})
		if err == nil {
			if count == 0 {
				e.runOnSuccess(ag)
				return proj.state, nil
			}
			final := &projection[T]{
				state:   ag.Value(),
				nextSeq: ag.nextSeq,
			}
			e.updateCache(id, final)
			e.runOnSuccess(ag)
			return final.state, nil
		}

		if !e.handleVersionConflict(err, id, proj) {
			return zero, err
		}
	}

	return zero, ErrMaxRetriesExceeded
}

func (e *Executor[T]) runOnSuccess(ag *Aggregator[T]) {
	val := ag.Value()
	for _, fn := range ag.success {
		func(cb SuccessAction[T]) {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("OnSuccess action panicked",
						slog.Any("aggregate_id", ag.id),
						slog.Any("panic", r))
				}
			}()
			cb(val)
		}(fn)
	}
}

// SaveSnapshot forces an immediate snapshot save for the given Aggregate
func (e *Executor[T]) SaveSnapshot(ctx context.Context, id AggregateID) error {
	proj, err := e.loadSnapshot(ctx, id)
	if err != nil {
		return err
	}
	return e.store.PutSnapshot(ctx, id, proj.state, proj.nextSeq)
}

func (e *Executor[T]) handleVersionConflict(
	err error, id AggregateID, proj *projection[T],
) bool {
	var versionErr *VersionConflictError
	if !errors.As(err, &versionErr) {
		return false
	}

	if evs := versionErr.NewEvents; len(evs) > 0 {
		updated := e.applyEvents(proj.state, evs, proj.nextSeq)
		e.updateCache(id, updated)
	}
	return true
}

func (e *Executor[T]) loadSnapshot(
	ctx context.Context, id AggregateID,
) (*projection[T], error) {
	key := id.Join(":")
	entry := e.cache.Get(key, func() *projection[T] {
		return &projection[T]{state: e.construct(), nextSeq: 0}
	})
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.value.nextSeq != 0 {
		return entry.value, nil
	}

	return e.loadFromStore(ctx, id, entry)
}

func (e *Executor[T]) loadFromStore(
	ctx context.Context, id AggregateID, entry *cacheEntry[*projection[T]],
) (*projection[T], error) {
	state := e.construct()

	snap, err := e.store.GetSnapshot(ctx, id, &state)
	if err != nil {
		return nil, err
	}

	proj := &projection[T]{
		state:   state,
		nextSeq: snap.NextSequence,
	}

	if len(snap.AdditionalEvents) > 0 {
		proj = e.applyEvents(state, snap.AdditionalEvents, snap.NextSequence)
	}

	if snap.ShouldSnapshot && e.store.snapshotWorker != nil {
		e.store.snapshotWorker.enqueue(id, proj.state, proj.nextSeq)
	}

	entry.value = proj
	return proj, nil
}

func (e *Executor[T]) applyEvents(st T, evs []*Event, startSeq int64) *projection[T] {
	for _, ev := range evs {
		if apply, ok := e.appliers[ev.Type]; ok {
			st = apply(st, ev)
		}
	}
	return &projection[T]{
		state:   st,
		nextSeq: startSeq + int64(len(evs)),
	}
}

func (e *Executor[T]) updateCache(id AggregateID, proj *projection[T]) {
	key := id.Join(":")

	entry := e.cache.Get(key, func() *projection[T] { return proj })
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if proj.nextSeq > entry.value.nextSeq {
		entry.value = proj
	}
}

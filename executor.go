package timebox

import (
	"context"
	"errors"
)

type (
	Executor[T any] struct {
		store      *Store
		appliers   Appliers[T]
		construct  constructor[T]
		cache      *lruCache[*projection[T]]
		maxRetries int
	}

	Command[T any] func(T, *Aggregator[T]) error

	projection[T any] struct {
		state   T
		nextSeq int64
	}
)

var ErrMaxRetriesExceeded = errors.New("max retries exceeded")

func NewExecutor[T any](
	tb *Timebox, apps Appliers[T], cons constructor[T],
) *Executor[T] {
	return &Executor[T]{
		store:      tb.store,
		appliers:   apps,
		construct:  cons,
		cache:      newLRUCache[*projection[T]](tb.config.CacheSize),
		maxRetries: tb.config.MaxRetries,
	}
}

func (e *Executor[T]) GetStore() *Store {
	return e.store
}

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
				return proj.state, nil
			}
			final := &projection[T]{
				state:   ag.Value(),
				nextSeq: ag.next,
			}
			e.updateCache(id, final)
			return final.state, nil
		}

		if !e.handleVersionConflict(err, id, proj) {
			return zero, err
		}
	}

	return zero, ErrMaxRetriesExceeded
}

// SaveSnapshot forces an immediate snapshot save for the given Aggregate
func (e *Executor[T]) SaveSnapshot(ctx context.Context, id AggregateID) error {
	proj, err := e.loadSnapshot(ctx, id)
	if err != nil {
		return err
	}
	return e.store.PutSnapshot(ctx, id, proj, proj.nextSeq)
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
	proj := &projection[T]{state: e.construct()}

	events, snapshot, err := e.store.GetSnapshot(ctx, id, proj)
	if err != nil {
		return nil, err
	}

	if len(events) > 0 {
		proj = e.applyEvents(proj.state, events, proj.nextSeq)
	}

	if snapshot && e.store.snapshotWorker != nil {
		e.store.snapshotWorker.enqueue(id, proj, proj.nextSeq)
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

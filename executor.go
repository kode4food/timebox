package timebox

import (
	"errors"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	// Executor orchestrates loading aggregate state, executing commands, and
	// persisting resulting events with optimistic retries
	Executor[T any] struct {
		store     *Store
		appliers  Appliers[T]
		construct constructor[T]
		cache     *cache[*projection[T]]
		success   []SuccessAction[T]
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

// NewExecutor constructs an Executor bound to a Store with the given appliers
// and state constructor
func NewExecutor[T any](
	store *Store, cons constructor[T], apps Appliers[T],
	onSuccess ...SuccessAction[T],
) *Executor[T] {
	return &Executor[T]{
		store:     store,
		appliers:  apps,
		construct: cons,
		cache:     newCache[*projection[T]](store.config.CacheSize),
		success:   onSuccess,
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
func (e *Executor[T]) Exec(id AggregateID, cmd Command[T]) (T, error) {
	var zero T
	for range e.store.config.MaxRetries {
		proj, err := e.loadSnapshot(id)
		if err != nil {
			return zero, err
		}

		ag := newAggregator(id, e.appliers, proj.state, proj.nextSeq)
		if err := cmd(ag.Value(), ag); err != nil {
			return zero, err
		}

		count, err := ag.flush(func(expectedSeq int64, evs []*Event) error {
			return e.store.AppendEvents(id, expectedSeq, evs)
		})
		if err == nil {
			if count == 0 {
				ag.runOnSuccess(e.success)
				return proj.state, nil
			}
			val := ag.Value()
			final := &projection[T]{
				state:   val,
				nextSeq: ag.nextSeq,
			}
			e.updateCache(id, final)
			ag.runOnSuccess(e.success)
			return final.state, nil
		}

		if !e.handleVersionConflict(err, id, proj) {
			return zero, err
		}
	}

	return zero, ErrMaxRetriesExceeded
}

// Get returns the current aggregate state
func (e *Executor[T]) Get(id AggregateID) (T, error) {
	return e.Exec(id, func(T, *Aggregator[T]) error {
		return nil
	})
}

// SaveSnapshot forces an immediate snapshot save for the given Aggregate
func (e *Executor[T]) SaveSnapshot(id AggregateID) error {
	var seq int64
	state, err := e.Exec(id, func(_ T, ag *Aggregator[T]) error {
		seq = ag.NextSequence()
		return nil
	})
	if err != nil {
		return err
	}
	return e.store.PutSnapshot(id, state, seq)
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

func (e *Executor[T]) loadSnapshot(id AggregateID) (*projection[T], error) {
	key := cacheKey(id)
	entry := e.cache.Get(key, func() *projection[T] {
		return &projection[T]{state: e.construct(), nextSeq: 0}
	})
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.value.nextSeq != 0 {
		return entry.value, nil
	}

	return e.loadFromStore(id, entry)
}

func (e *Executor[T]) loadFromStore(
	id AggregateID, entry *cacheEntry[*projection[T]],
) (*projection[T], error) {
	state := e.construct()

	snap, err := e.store.GetSnapshot(id, &state)
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

	if e.shouldSnapshot(snap) {
		err := e.store.PutSnapshot(id, proj.state, proj.nextSeq)
		if err != nil {
			return nil, err
		}
	}

	entry.value = proj
	return proj, nil
}

func (e *Executor[T]) applyEvents(
	st T, evs []*Event, startSeq int64,
) *projection[T] {
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

func (e *Executor[_]) shouldSnapshot(snap *SnapshotResult) bool {
	if len(snap.AdditionalEvents) == 0 {
		return false
	}
	if snap.SnapshotSize == 0 {
		return true
	}
	rat := float64(snap.EventsSize) / float64(snap.SnapshotSize)
	return rat > e.store.config.SnapshotRatio
}

func (e *Executor[T]) updateCache(id AggregateID, proj *projection[T]) {
	key := cacheKey(id)

	entry := e.cache.Get(key, func() *projection[T] { return proj })
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if proj.nextSeq > entry.value.nextSeq {
		entry.value = proj
	}
}

func cacheKey(id AggregateID) string {
	n := len(id) * 4
	for _, part := range id {
		n += len(part)
	}

	buf := make([]byte, 0, n)
	for _, part := range id {
		buf = bin.AppendString(buf, string(part))
	}
	return string(buf)
}

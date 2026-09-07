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

// Executor constructs an Executor bound to a Store with the given appliers
// and state constructor
func (s *Store) Executor[T any](
	cons constructor[T], apps Appliers[T], onSuccess ...SuccessAction[T],
) *Executor[T] {
	return &Executor[T]{
		store:     s,
		appliers:  apps,
		construct: cons,
		cache:     newCache[*projection[T]](s.config.CacheSize),
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
	var res T
	if err := e.store.Transaction(func(t *Transaction) error {
		var err error
		res, err = t.Exec(e, id, cmd)
		return err
	}); err != nil {
		var zero T
		return zero, err
	}
	return res, nil
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

// complete refreshes the cached projection and runs success actions once the
// Transaction holding this aggregate has committed
func (e *Executor[T]) complete(id AggregateID, ag *Aggregator[T]) {
	if len(ag.flushed) > 0 {
		e.updateCache(id, &projection[T]{
			state:   ag.Value(),
			nextSeq: ag.nextSeq,
		})
	}
	ag.runOnSuccess(e.success)
}

func (e *Executor[T]) invalidate(id AggregateID) {
	entry := e.cache.Get(cacheKey(id), func() *projection[T] {
		return &projection[T]{state: e.construct()}
	})
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.value = &projection[T]{state: e.construct()}
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
	st := e.construct()

	snap, err := e.store.GetSnapshot(id, &st)
	if err != nil {
		return nil, err
	}

	proj := &projection[T]{
		state:   st,
		nextSeq: snap.NextSequence,
	}

	if len(snap.AdditionalEvents) > 0 {
		proj = e.applyEvents(st, snap.AdditionalEvents, snap.NextSequence)
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

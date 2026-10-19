package timebox

import (
	"encoding/json"
	"errors"

	bin "github.com/kode4food/timebox/internal/binary"
)

type (
	// Executor orchestrates loading aggregate state, executing commands, and
	// persisting resulting events with optimistic retries
	Executor[T any] struct {
		store      *Store
		reducer    Reducer[T]
		construct  Constructor[T]
		cache      *cache[*projection[T]]
		success    []SuccessAction[T]
		trimEvents bool
	}

	// Command is user code that inspects state and raises events on an
	// Aggregator. Returning an error aborts the operation
	Command[T any] func(T, *Aggregator[T]) error

	// Constructor instantiates initial aggregate state
	Constructor[T any] func() T

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

// Executor constructs an Executor bound to a Store with the given reducer and
// state constructor
func (s *Store) Executor[T any](
	cons Constructor[T], reducer Reducer[T], onSuccess ...SuccessAction[T],
) *Executor[T] {
	return &Executor[T]{
		store:      s,
		reducer:    reducer,
		construct:  cons,
		cache:      newCache[*projection[T]](s.config.CacheSize),
		success:    onSuccess,
		trimEvents: s.config.TrimEvents,
	}
}

// Exec loads the aggregate state, executes the command, and persists raised
// events. It retries on version conflicts up to MaxRetries
func (e *Executor[T]) Exec(id AggregateID, cmd Command[T]) (T, error) {
	var res T
	if err := e.store.Transact(func(t *Transaction) error {
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
	return e.putSnapshot(id, state, seq)
}

func (s *Store) newInternalExecutor[T any](
	cons Constructor[T], reducer Reducer[T], onSuccess ...SuccessAction[T],
) *Executor[T] {
	res := s.Executor(cons, reducer, onSuccess...)
	res.trimEvents = true
	return res
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
	proj, snapshot, err := e.readProjection(id)
	if err != nil {
		return nil, err
	}

	if snapshot {
		err := e.putSnapshot(id, proj.state, proj.nextSeq)
		if err != nil {
			return nil, err
		}
	}

	entry.value = proj
	return proj, nil
}

// readProjection replays an aggregate's snapshot and remaining events into its
// current projection, with no cache or snapshot side effects
func (e *Executor[T]) readProjection(
	id AggregateID,
) (*projection[T], bool, error) {
	rec, err := e.store.backend.LoadSnapshot(LoadSnapshotRequest{ID: id})
	if err != nil {
		return nil, false, err
	}

	st := e.construct()
	if len(rec.Data) > 0 {
		st, err = decodeJSON[T](rec.Data)
		if err != nil {
			return nil, false, err
		}
	}
	proj := &projection[T]{state: st, nextSeq: rec.Sequence}
	if len(rec.Events) > 0 {
		proj = e.applyEvents(st, rec.Events, rec.Sequence)
	}
	return proj, e.shouldSnapshot(rec), nil
}

func (e *Executor[T]) applyEvents(
	st T, evs []*Event, startSeq int64,
) *projection[T] {
	return &projection[T]{
		state:   e.reducer(st, evs),
		nextSeq: startSeq + int64(len(evs)),
	}
}

func (e *Executor[_]) shouldSnapshot(rec *SnapshotRecord) bool {
	if len(rec.Events) == 0 {
		return false
	}
	if len(rec.Data) == 0 {
		return true
	}
	eventsSize := 0
	for _, ev := range rec.Events {
		eventsSize += len(ev.Data)
	}
	rat := float64(eventsSize) / float64(len(rec.Data))
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

func (e *Executor[T]) putSnapshot(
	id AggregateID, value T, sequence int64,
) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return e.store.backend.SaveSnapshot(SnapshotRequest{
		ID:         id,
		Data:       data,
		Sequence:   sequence,
		TrimEvents: e.trimEvents,
	})
}

func cacheKey(id AggregateID) string {
	buf := make([]byte, 0, len(id.Type)+len(id.Key)+8)
	buf = bin.AppendString(buf, string(id.Type))
	buf = bin.AppendString(buf, string(id.Key))
	return string(buf)
}

package timebox

import (
	"errors"
	"maps"
)

type (
	// Transaction collects the append intent of several Aggregators and
	// commits it as a single atomic append. It is not safe for concurrent use
	Transaction struct {
		store *Store
		parts []*participant
	}

	// participant holds one aggregate's staged append intent along with the
	// type-erased actions its Executor runs after a commit attempt
	participant struct {
		aggregator any
		complete   func()
		reset      func([]*Event)
		request    AppendRequest
		id         AggregateID
		staged     bool
	}
)

var (
	// ErrStoreMismatch indicates a Transaction was given an Executor bound to
	// a different Store
	ErrStoreMismatch = errors.New("executor belongs to a different store")

	// ErrAggregateTypeConflict indicates one aggregate was joined twice under
	// conflicting state types
	ErrAggregateTypeConflict = errors.New(
		"aggregate joined with conflicting state types",
	)
)

// Transact runs fn and commits every aggregate joined through
// Transaction.Exec as one atomic append. It retries fn on version conflict up
// to MaxRetries. An error returned from fn discards the transaction
func (s *Store) Transact(fn func(*Transaction) error) error {
	for range s.config.MaxRetries {
		t := &Transaction{store: s}
		if err := fn(t); err != nil {
			return err
		}
		err := t.commit()
		if err == nil {
			for _, p := range t.parts {
				p.complete()
			}
			return nil
		}
		var confErr *VersionConflictError
		if !errors.As(err, &confErr) {
			return err
		}
		t.reset(confErr)
	}
	return ErrMaxRetriesExceeded
}

// Exec runs cmd against the aggregate and enlists the resulting events in the
// Transaction, continuing any Aggregator already joined for it. The returned
// value holds only if the Transaction commits
func (t *Transaction) Exec[T any](
	e *Executor[T], id AggregateID, cmd Command[T],
) (T, error) {
	var zero T
	if e.store != t.store {
		return zero, ErrStoreMismatch
	}
	p, ag, err := t.join(e, id)
	if err != nil {
		return zero, err
	}
	if err := cmd(ag.Value(), ag); err != nil {
		return zero, err
	}
	if _, err := ag.flush(func(atSeq int64, evs []*Event) error {
		p.stage(t.store.appendRequest(id, atSeq, evs))
		return nil
	}); err != nil {
		return zero, err
	}
	return ag.Value(), nil
}

func (t *Transaction) join[T any](
	e *Executor[T], id AggregateID,
) (*participant, *Aggregator[T], error) {
	for _, p := range t.parts {
		if !p.id.Equal(id) {
			continue
		}
		ag, ok := p.aggregator.(*Aggregator[T])
		if !ok {
			return nil, nil, ErrAggregateTypeConflict
		}
		return p, ag, nil
	}

	proj, err := e.loadSnapshot(id)
	if err != nil {
		return nil, nil, err
	}
	ag := newAggregator(id, e.appliers, proj.state, proj.nextSeq)
	ag.tx = t

	p := &participant{
		aggregator: ag,
		id:         id,
		complete:   func() { e.complete(id, ag) },
		reset: func(evs []*Event) {
			if len(evs) == 0 {
				e.invalidate(id)
				return
			}
			e.updateCache(id, e.applyEvents(proj.state, evs, proj.nextSeq))
		},
	}
	t.parts = append(t.parts, p)
	return p, ag, nil
}

func (t *Transaction) commit() error {
	reqs := make([]AppendRequest, 0, len(t.parts))
	for _, p := range t.parts {
		if p.staged {
			reqs = append(reqs, p.request)
		}
	}
	if len(reqs) == 0 {
		return nil
	}
	return t.store.persistence.Append(reqs...)
}

// reset refreshes cached projections after a failed commit attempt. Only the
// conflicting aggregate may adopt the conflict's events
func (t *Transaction) reset(confErr *VersionConflictError) {
	for _, p := range t.parts {
		if p.id.Equal(confErr.ID) {
			p.reset(confErr.NewEvents)
			continue
		}
		p.reset(nil)
	}
}

func (p *participant) stage(req AppendRequest) {
	if !p.staged {
		p.request = req
		p.staged = true
		return
	}
	p.request.Events = append(p.request.Events, req.Events...)
	if req.Status != nil {
		p.request.Status = req.Status
		p.request.StatusAt = req.StatusAt
	}
	maps.Copy(p.request.Tags, req.Tags)
}

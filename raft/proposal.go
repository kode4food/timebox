package raft

import (
	"context"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/kode4food/timebox"
)

func (p *Persistence) propose(
	ctx context.Context, data []byte, proposalID uint64,
	events []*timebox.Event,
) (*ApplyResult, error) {
	st := p.registerProposal(proposalID, events)
	defer p.unregisterProposal(proposalID, st)

	if err := p.node.Propose(ctx, data); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.stopCh:
		if err, ok := p.stopErr.Load().(error); ok &&
			err != nil {
			return nil, err
		}
		return nil, raft.ErrStopped
	case res := <-st.ch:
		if err := res.Err(); err != nil {
			return nil, err
		}
		return res, nil
	}
}

func (p *Persistence) registerProposal(
	id uint64, events []*timebox.Event,
) proposalState {
	st := proposalState{
		ch:     make(chan *ApplyResult, 1),
		events: append([]*timebox.Event(nil), events...),
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	p.pending[id] = st
	return st
}

func (p *Persistence) unregisterProposal(id uint64, st proposalState) {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	if cur, ok := p.pending[id]; ok && cur.ch == st.ch {
		delete(p.pending, id)
	}
}

func (p *Persistence) resolveProposal(proposalID uint64, res *ApplyResult) {
	if proposalID == 0 {
		return
	}
	p.pendingMu.Lock()
	st, ok := p.pending[proposalID]
	if ok {
		delete(p.pending, proposalID)
	}
	p.pendingMu.Unlock()
	if ok {
		st.ch <- res
	}
}

func (p *Persistence) proposalEvents(proposalID uint64) []*timebox.Event {
	if proposalID == 0 {
		return nil
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	st, ok := p.pending[proposalID]
	if !ok || len(st.events) == 0 {
		return nil
	}
	evs := st.events
	st.events = nil
	p.pending[proposalID] = st
	return evs
}

func (p *Persistence) applyWithTimeout(
	ctx context.Context, data []byte, proposalID uint64,
	events []*timebox.Event,
) (*ApplyResult, error) {
	timeout, err := p.commandTimeout(ctx)
	if err != nil {
		return nil, err
	}
	proposeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return p.propose(proposeCtx, data, proposalID, events)
}

func (p *Persistence) commandTimeout(
	ctx context.Context,
) (time.Duration, error) {
	timeout := p.applyTimeout
	if dl, ok := ctx.Deadline(); ok {
		if rem := time.Until(dl); rem < timeout {
			timeout = rem
		}
	}
	if timeout <= 0 {
		return 0, ctx.Err()
	}
	return timeout, nil
}

func (p *Persistence) newProposalID() uint64 {
	return p.nextProposal.Add(1)
}

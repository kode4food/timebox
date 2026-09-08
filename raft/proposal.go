package raft

import (
	"bytes"
	"context"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/kode4food/timebox"
)

func (b *Backend) propose(
	ctx context.Context, data []byte, proposalID uint64,
	events []*timebox.Event,
) (*ApplyResult, error) {
	select {
	case <-b.stopCh:
		if err, ok := b.stopErr.Load().(error); ok && err != nil {
			return nil, err
		}
		return nil, raft.ErrStopped
	default:
	}

	st := b.registerProposal(proposalID, data, events)
	defer b.unregisterProposal(proposalID, st)

	if err := b.node.Propose(ctx, data); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.stopCh:
		if err, ok := b.stopErr.Load().(error); ok && err != nil {
			return nil, err
		}
		return nil, raft.ErrStopped
	case res := <-st.ch:
		if res.Error != nil {
			return nil, res.Error
		}
		return res, nil
	}
}

func (b *Backend) registerProposal(
	id uint64, data []byte, events []*timebox.Event,
) proposalState {
	st := proposalState{
		ch:     make(chan *ApplyResult, 1),
		cmd:    append(Command(nil), data...),
		events: append([]*timebox.Event(nil), events...),
	}
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	b.pending[id] = st
	return st
}

func (b *Backend) unregisterProposal(id uint64, st proposalState) {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	if cur, ok := b.pending[id]; ok && cur.ch == st.ch {
		delete(b.pending, id)
	}
}

func (b *Backend) resolveProposal(
	proposalID uint64, cmd Command, res *ApplyResult,
) {
	if proposalID == 0 {
		return
	}
	b.pendingMu.Lock()
	st, ok := b.pending[proposalID]
	if ok && proposalMatches(st, cmd) {
		delete(b.pending, proposalID)
	} else {
		ok = false
	}
	b.pendingMu.Unlock()
	if ok {
		st.ch <- res
	}
}

func (b *Backend) proposalEvents(
	proposalID uint64, cmd Command,
) []*timebox.Event {
	if proposalID == 0 {
		return nil
	}
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	st, ok := b.pending[proposalID]
	if !ok || !proposalMatches(st, cmd) || len(st.events) == 0 {
		return nil
	}
	evs := st.events
	st.events = nil
	b.pending[proposalID] = st
	return evs
}

func (b *Backend) applyWithTimeout(
	ctx context.Context, data []byte, proposalID uint64,
	events []*timebox.Event,
) (*ApplyResult, error) {
	timeout, err := b.commandTimeout(ctx)
	if err != nil {
		return nil, err
	}
	proposeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return b.propose(proposeCtx, data, proposalID, events)
}

func (b *Backend) commandTimeout(
	ctx context.Context,
) (time.Duration, error) {
	timeout := b.applyTimeout
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

func (b *Backend) newProposalID() uint64 {
	return b.nextProposal.Add(1)
}

func proposalMatches(st proposalState, cmd Command) bool {
	return bytes.Equal(st.cmd, cmd)
}

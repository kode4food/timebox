package raft

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
)

func (p *Persistence) serveForwarding() {
	p.bgWG.Go(func() {

		for {
			conn, err := p.forwarder.Accept()
			if err != nil {
				if errors.Is(err, ErrTransportClosed) ||
					errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}

			p.bgWG.Go(func() {
				p.handleForwardConn(conn)
			})
		}
	})
}

func (p *Persistence) handleForwardConn(conn net.Conn) {
	defer func() {
		p.forwarder.releaseConn(conn)
		_ = conn.Close()
	}()

	rd := bufio.NewReader(conn)
	wr := bufio.NewWriter(conn)

	for {
		timeout, data, err := readForwardRequest(rd)
		if err != nil {
			if errors.Is(err, io.EOF) ||
				errors.Is(err, io.ErrUnexpectedEOF) ||
				errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}

		if timeout <= 0 {
			timeout = p.applyTimeout
		}

		ctx, cancel := context.WithTimeout(
			context.Background(), timeout,
		)
		res, err := p.applyEncodedAsLeader(ctx, data)
		cancel()

		if err := writeForwardResponse(wr, res, err); err != nil {
			return
		}
	}
}

func (p *Persistence) forwardCommand(
	ctx context.Context, cmd command,
) (*applyResult, error) {
	timeout, err := p.commandTimeout(ctx)
	if err != nil {
		return nil, err
	}

	leaderAddr, leaderID := p.LeaderWithID()
	forwardAddr := p.forwardAddress(leaderID, leaderAddr)
	if forwardAddr == "" {
		return nil, ErrNotLeader
	}

	data, err := encodeCommand(cmd)
	if err != nil {
		return nil, err
	}

	pool, err := p.forwardPoolFor(forwardAddr)
	if err != nil {
		return nil, err
	}

	res, err := pool.Call(ctx, timeout, data)
	if errors.Is(err, ErrTransportClosed) {
		return nil, ErrNotLeader
	}
	return res, err
}

func (p *Persistence) forwardAddress(
	leaderID ServerID, leaderAddr ServerAddress,
) ServerAddress {
	if leaderID != "" {
		if addr, ok := p.forwardByID[leaderID]; ok {
			return addr
		}
	}
	if leaderAddr != "" {
		if addr, ok := p.forwardByRaftAddr[leaderAddr]; ok {
			return addr
		}
	}
	return ""
}

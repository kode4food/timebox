package raft

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type (
	forwardConn struct {
		conn net.Conn
		rd   *bufio.Reader
		wr   *bufio.Writer
		once sync.Once
		err  error
	}

	forwardPool struct {
		server *forwardServer
		addr   ServerAddress
		conns  map[*forwardConn]struct{}
		idle   chan *forwardConn
		open   chan struct{}
		done   chan struct{}
		mu     sync.Mutex
		closed bool
	}
)

const (
	defaultForwardMaxOpen = 128
	defaultForwardIdle    = defaultForwardMaxOpen
)

func (p *forwardPool) Call(
	ctx context.Context, timeout time.Duration, data []byte,
) (*applyResult, error) {
	conn, err := p.acquire(ctx, timeout)
	if err != nil {
		return nil, err
	}

	res, err := conn.Call(timeout, data)
	if err != nil {
		p.drop(conn)
		return nil, err
	}

	p.release(conn)
	return res, nil
}

func (p *forwardPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.done)
	conns := make([]*forwardConn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()

	var errs []error
	for _, conn := range conns {
		if conn == nil {
			continue
		}
		errs = append(errs, p.closeConn(conn))
	}
	return errors.Join(errs...)
}

func (p *forwardPool) acquire(
	ctx context.Context, timeout time.Duration,
) (*forwardConn, error) {
	for {
		select {
		case conn := <-p.idle:
			if conn != nil {
				return conn, nil
			}
			continue
		default:
		}

		if err := p.checkOpen(ctx); err != nil {
			return nil, err
		}

		select {
		case p.open <- struct{}{}:
			conn, err := p.openConn(timeout)
			if err == nil {
				return conn, nil
			}
			p.releasePermit()
			if errors.Is(err, ErrTransportClosed) ||
				errors.Is(err, net.ErrClosed) {
				return nil, err
			}
			return nil, err
		default:
		}

		select {
		case conn := <-p.idle:
			if conn != nil {
				return conn, nil
			}
		case p.open <- struct{}{}:
			conn, err := p.openConn(timeout)
			if err == nil {
				return conn, nil
			}
			p.releasePermit()
			if errors.Is(err, ErrTransportClosed) ||
				errors.Is(err, net.ErrClosed) {
				return nil, err
			}
			return nil, err
		case <-p.done:
			return nil, ErrTransportClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *forwardPool) release(conn *forwardConn) {
	if conn == nil {
		return
	}

	select {
	case <-p.done:
		_ = p.closeConn(conn)
		return
	case p.idle <- conn:
	default:
		_ = p.closeConn(conn)
	}
}

func (p *forwardPool) drop(conn *forwardConn) {
	if conn == nil {
		return
	}
	_ = p.closeConn(conn)
}

func (p *forwardPool) closeConn(conn *forwardConn) error {
	if conn == nil {
		return nil
	}
	if !p.untrack(conn) {
		return nil
	}

	err := conn.Close()
	p.releasePermit()
	return err
}

func (p *forwardPool) checkOpen(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-p.done:
		return ErrTransportClosed
	default:
	}

	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return ErrTransportClosed
	}
	return nil
}

func (p *forwardPool) releasePermit() {
	select {
	case <-p.open:
	default:
	}
}

func (p *forwardPool) openConn(timeout time.Duration) (*forwardConn, error) {
	conn, err := newForwardConn(p.server, p.addr, timeout)
	if err != nil {
		return nil, err
	}
	if !p.track(conn) {
		_ = conn.Close()
		return nil, ErrTransportClosed
	}
	return conn, nil
}

func (p *forwardPool) track(conn *forwardConn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false
	}
	p.conns[conn] = struct{}{}
	return true
}

func (p *forwardPool) untrack(conn *forwardConn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.conns[conn]; !ok {
		return false
	}
	delete(p.conns, conn)
	return true
}

func (c *forwardConn) Close() error {
	if c.conn == nil {
		return nil
	}
	c.once.Do(func() {
		c.err = c.conn.Close()
	})
	return c.err
}

func (c *forwardConn) Call(
	timeout time.Duration, data []byte,
) (*applyResult, error) {
	if err := c.conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	if err := writeForwardRequest(c.wr, timeout, data); err != nil {
		return nil, err
	}
	return readForwardResponse(c.rd)
}

func newForwardPool(server *forwardServer, addr ServerAddress) *forwardPool {
	return &forwardPool{
		server: server,
		addr:   addr,
		conns:  map[*forwardConn]struct{}{},
		idle:   make(chan *forwardConn, defaultForwardIdle),
		open:   make(chan struct{}, defaultForwardMaxOpen),
		done:   make(chan struct{}),
	}
}

func newForwardConn(
	server *forwardServer, addr ServerAddress, timeout time.Duration,
) (*forwardConn, error) {
	conn, err := server.Dial(addr, timeout)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	return &forwardConn{
		conn: conn,
		rd:   bufio.NewReader(conn),
		wr:   bufio.NewWriter(conn),
	}, nil
}

package raft

import (
	"errors"
	"net"
	"sync"
	"time"
)

type forwardServer struct {
	advertise ServerAddress
	listener  *net.TCPListener
	mu        sync.Mutex
	conns     map[net.Conn]struct{}
}

func (s *forwardServer) Accept() (net.Conn, error) {
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	s.trackConn(conn)
	return conn, nil
}

func (s *forwardServer) Close() error {
	errs := []error{s.listener.Close()}
	for _, conn := range s.takeConns() {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

func (s *forwardServer) Dial(
	addr ServerAddress, timeout time.Duration,
) (net.Conn, error) {
	return net.DialTimeout("tcp", string(addr), timeout)
}

func (s *forwardServer) Addr() net.Addr {
	if s.advertise != "" {
		addr, err := net.ResolveTCPAddr("tcp", string(s.advertise))
		if err == nil {
			return addr
		}
	}
	return s.listener.Addr()
}

func (s *forwardServer) ServerAddress() ServerAddress {
	if s.advertise != "" {
		return s.advertise
	}
	if addr := s.Addr(); addr != nil {
		return ServerAddress(addr.String())
	}
	return ""
}

func (s *forwardServer) releaseConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}

func (s *forwardServer) trackConn(conn net.Conn) {
	s.mu.Lock()
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
}

func (s *forwardServer) takeConns() []net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.conns) == 0 {
		return nil
	}

	res := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		res = append(res, conn)
	}
	s.conns = map[net.Conn]struct{}{}
	return res
}

func newForwardServer(cfg Config) (*forwardServer, error) {
	if cfg.ForwardBindAddress == "" {
		return nil, nil
	}

	ln, err := net.Listen("tcp", cfg.ForwardBindAddress)
	if err != nil {
		return nil, err
	}

	s := &forwardServer{
		advertise: ServerAddress(cfg.ForwardAddress()),
		listener:  ln.(*net.TCPListener),
		conns:     map[net.Conn]struct{}{},
	}

	return s, nil
}

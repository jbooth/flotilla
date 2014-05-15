package raft

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
	code_raft          = 1 // used to connect to raft upon accept()
	code_flotilla      = 2 // used to connect to flotilla upon accept()
)

// binds to a socket and uses normal TCP for all ops
func NewTCPTransportStandard(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*NetworkTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return NewTCPTransport(list, defaultDialer,
		advertise, maxPool, timeout, logOutput)
}

// Uses the provided list and dial functions, can be used to layer over another protocol
func NewTCPTransportCustom(
	list net.Listener,
	dial func(net.Addr, time.Duration) (net.Conn, error),
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*NetworkTransport, error) {

	// sanity checks for listener and advertised address
	tcpBindAddr, ok := list.Addr().(*net.TCPAddr)
	if !ok {
		log.Printf("TCPTransport Warning: listener on non TCP address %s", advertise)
	} else if tcpBindAddr.IP.IsUnspecified() {
		log.Printf("TCPTransport Warning, listener on non advertisable IP: %s", tcpAdvertise.IP)
	}
	if advertise != nil {
		tcpAdvertise, ok := advertise.(*net.TCPAddr)
		if !ok {
			log.Printf("TCPTransport Warning: advertising non TCP address %s", advertise)
		} else if tcpAdvertise.IP.IsUnspecified() {
			log.Printf("TCPTransport Warning, non advertisable IP: %s", tcpAdvertise.IP)
		}
	}

	// Create stream layer for raft
	raftStream := &TCPStreamLayer{
		advertise: advertise,
		listener:  list,
		dialer: func(a net.Addr, t time.Duration) (net.Conn, error) {
			return dialWithCode(dial, code_raft, a, t)
		},
	}

	// Create the raft network transport
	trans := NewNetworkTransport(stream, maxPool, timeout, logOutput)
	return trans, nil
}

type FlotillaCommandClient struct {
	dialer func(a net.Addr, t time.Duration) (net.Conn, error)
	conns  map[net.Addr]conn
	l      *sync.Mutex
}

type flotillaCommandClientConn struct {
}

func defaultDialer(a net.Addr, t time.Duration) (net.Conn, error) {
	return net.DialTimeout(a, t)
}

func dialWithCode(dialer func(net.Addr, time.Duration) (net.Conn, error), code byte, address net.Addr, timeout net.Timeout) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	_, err := conn.Write([]byte{code})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

type router struct {
	listen        net.Listener
	newConns      chan net.Conn
	closeRequests chan closeReq
	chans         map[byte]*chanListener
	l             *sync.RWMutex
}

func (r *router) serve() {
	// dispatch func to feed new connections into r.newConns
	go func() {
		for {
			conn, err := r.listen.Accept()
			if err != nil {
				log.Printf("router.serve(): Error accepting on %s : %s", r.listen.Addr(), err)
				return
			}
			r.newConns <- conn
		}
	}()
	code := make([]byte, 1)
	for {
		// select between r.newConns and closeRequests
		select {
		case conn, ok := <-r.newConns:
			if !ok {
				log.Printf("Router closed, ending accept loop")
				return
			}

			_, err := conn.Read(code)
			if err != nil {
				log.Printf("Error reading from conn %s, discarding.  Error: %s", conn.RemoteAddr(), err)
				conn.Close()
				continue
			}
			r.chans[code[0]].conns <- conn
		case toClose := <-r.closeRequests:
			close(r.chans[toClose.code].conns)
			delete(r.chans, toClose.code)
			toClose.resp <- true
		}
	}
}

type closeReq struct {
	code byte
	resp chan bool
}
type chanListener struct {
	r      *router
	addr   net.Addr
	conns  chan net.Conn
	closed bool
	myCode byte
}

// Accept waits for and returns the next connection to the listener.
func (c *chanListener) Accept() (c net.Conn, err error) {
	c, ok := <-c.conns
	if !ok {
		return nil, fmt.Errorf("Tried to accept from closed chan on %s", c.addr)
	}
	return c, nil
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (c *chanListener) Close() error {
	req := closeReq{
		c.myCode,
		make(chan bool, 1),
	}
	c.r.closeRequests <- req
	<-req.resp
	return nil
}

// Addr returns the listener's network address.
func (c *chanListener) Addr() Addr {
	return c.addr
}

// TCPStreamLayer implements raft.StreamLayer interface
type TCPStreamLayer struct {
	advertise net.Addr
	listener  net.Listener
	dialer    func(address string, timeout time.Duration) (net.Conn, error)
}

// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	return t.dialer(address, timeout)
}

// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

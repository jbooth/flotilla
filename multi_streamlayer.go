package flotilla

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
	code_raft          = 1 // used to connect to raft upon accept()
	code_flotilla      = 2 // used to connect to flotilla upon accept()
)

// For each unique serviceCode provided, returns a transport layer with accept()
// and dial() functions by connecting to the provided addr and 'dialing' the byte.
//
// Connections are obtained from the returned transport layer's net.Listener interface.
func NewStdMultiStream(
	bindAddr string,
	lg log.Logger,
	serviceCodes ...byte,
) (*NetworkTransport, error) {
	// Try to bind
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return NewMultiStream(list, defaultDialer,
		list.Addr(), lg, serviceCodes)
}

func defaultDialer(a net.Addr, t time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", a, t)
}

// Uses the provided list and dial functions, can be used to allow flotilla
// to share a bound port with another binary service.
func NewMultiStream(
	list net.Listener,
	dial func(net.Addr, time.Duration) (net.Conn, error),
	advertise net.Addr,
	lg *log.Logger,
	serviceCodes ...byte,
) (map[byte]raft.StreamLayer, error) {
	// sanity checks for listener and advertised address
	if advertise == nil {
		advertise = list.Addr()
	}
	tcpAdvertise, ok := advertise.(*net.TCPAddr)
	if !ok {
		log.Printf(logOutput, "NewMultiStream Warning: advertising non TCP address %s", advertise)
	} else if tcpAdvertise.IP.IsUnspecified() {
		log.Printf(logOutput, "NewMultiStream Warning: advertising unspecified IP: %s", tcpAdvertise.IP)
	}
	// set up router
	r := &router{listen, make(chan net.Conn, 16), make(chan closeReq, 16), make(map[byte]*serviceStreams), lg}
	// set up a channel of conns for each unique serviceCode
	for _, b := range opCodes {
		_, exists := services[b]
		if exists {
			continue
		}
		r.chans[b] = &serviceStreams{
			r:      r,
			addr:   advertise,
			conns:  make(chan net.Conn, 16),
			closed: false,
			myCode: b,
			dial:   dial,
		}
	}
	go r.serve()
	return r.chans, nil
}

type router struct {
	listen        net.Listener
	newConns      chan net.Conn
	closeRequests chan closeReq
	chans         map[byte]*serviceStreams
	lg            *log.Logger
}

func (r *router) serve() {
	// dispatch func to feed new connections into r.newConns
	go func() {
		for {
			conn, err := r.listen.Accept()
			if err != nil {
				r.lg.Printf("router.serve(): Error accepting on %s : %s", r.listen.Addr(), err)
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
				r.lg.Printf("Router closed, ending accept loop")
				return
			}

			_, err := conn.Read(code)
			if err != nil {
				r.lg.Printf("Error reading from conn %s, discarding.  Error: %s", conn.RemoteAddr(), err)
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

// serviceStreams implements raft.StreamLayer interface for a given service code
// we dial to remote hosts by connecting and then sending the code
// we accept conns through the router that created us, by code
type serviceStreams struct {
	r      *router
	addr   net.Addr
	conns  chan net.Conn
	closed bool
	myCode byte
	dial   func(address net.Addr, timeout time.Duration) (net.Conn, error)
}

// Accept waits for and returns the next connection to the listener.
func (c *serviceStreams) Accept() (c net.Conn, err error) {
	c, ok := <-c.conns
	if !ok {
		return nil, fmt.Errorf("Tried to accept from closed chan on %s", c.addr)
	}
	return c, nil
}

// Close closes the listener.
// Any blocked Accept operations will be unblocked and return errors.
func (c *serviceStreams) Close() error {
	req := closeReq{
		c.myCode,
		make(chan bool, 1),
	}
	c.r.closeRequests <- req
	<-req.resp
	return nil
}

// Addr returns the listener's network address.
func (c *serviceStreams) Addr() Addr {
	return c.addr
}

// connects to the service listening on
func (c *serviceStreams) Dial(address net.Addr, timeout time.Duration) (net.Conn, error) {
	return dialWithCode(c.dial, c.myCode, address, timeout)
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

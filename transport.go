package flotilla

import (
	"bytes"
	"github.com/jbooth/flotilla/raft"
	"github.com/ugorji/go/codec"
	"log"
	"net"
	"sync"
)

// flotilla client connection
type connToLeader struct {
	c       net.Conn
	e       codec.Encoder
	d       codec.Decoder
	l       *sync.Mutex
	lg      *log.Logger
	pending chan *commandCallback
}

// joins the raft leader and sets up infrastructure for
// processing commands
// can return ErrNotLeader
func newConnToLeader(conn net.Conn, lg *log.Logger) (*connToLeader, error) {
	// send join command
	h := codec.MsgpackHandle{}
	ret := &connToLeader{
		c:       conn,
		e:       codec.NewEncoder(conn, h),
		d:       codec.NewDecoder(conn, h),
		l:       new(sync.Mutex),
		lg:      lg,
		pending: make(chan *commandCallback, 64),
	}
	join := &joinReq{
		peerAddr: advertiseAddr,
	}
	err := ret.e.Encode(join)
	if err != nil {
		ret.c.Close()
		return nil, err
	}
	joinResp := &joinResp{}
	err = ret.d.Decode(joinResp)
	if err != nil {
		ret.lg.Printf("Error connecting to leader at %s : %s", conn.RemoteAddr().String(), err)
		ret.c.Close()
		return nil, err
	}
	go ret.readResponses()
	return ret, nil
}

// sends the command for remote execution.
// returns error if we couldn't communicate with leader
func (c *connToLeader) forwardCommand(cb *commandCallback, cmdName string, args [][]byte) error {
	c.l.Lock()
	defer c.l.Unlock()
	// marshal log object
	lg := logForCommand(cb.originAddr, cb.reqNo, cmdName, args)
	resp := make(chan error, 1)
	// put response chan in pending
	// send
	err = c.e.Encode(lg)
	if err != nil {
		cb.cancel()
		cb.result <- &Result{nil, err}
		return err
	} else {
		// so our responseReader will forward appropriately
		c.pending <- cb
	}
	return nil
}

func (c *connToLeader) remoteAddr() net.Addr {
	return c.c.RemoteAddr()
}

func (c *connToLeader) readResponses() {
	resp := &commandResp{}
	for cb, ok := range c.pending {
		if !ok {
			c.lg.Printf("Closing leaderConn to %s", c.c.RemoteAddr().String())
			return
		}
		err = c.d.Decode(resp)
		if err != nil {
			cb.cancel()
			cb <- Result{nil, err}
			c.lg.Printf("Error reading response: %s, closing and giving err to all pending requests", err)
			c.c.Close()
			for {
				select {
				case cb1 := <-c.pending:
					cb1.cancel()
					cb1 <- Result{nil, err}
				default:
					return
				}
			}
		}
	}
}

func bytesForCommand(host string, reqno uint64, cmdName string, args [][]byte) []byte {
	cmd := &commandReq{}
	cmd.Args = args
	cmd.Cmd = cmdName
	// no callback
	cmd.OriginAddr = host
	cmd.Reqno = reqno
	b, err := encodeMsgPack(cmd)
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}

func logForCommand(host string, reqno uint64, cmdName string, args [][]byte) *raft.Log {
	return &raft.Log{
		Index: 0,
		Term:  0,
		Type:  raft.LogCommand,
		Data:  bytesForCommand(host, reqno, cmdName, args),
	}
}

// serves a follower from a leader server
// we tell all servers to go elsewhere if we are not leader
func serveFollower(lg *log.Logger, follower net.Conn, leader *server) {
	ch := codec.MsgpackHandle{}
	decode := codec.NewDecoder(follower, ch)
	encode := codec.NewEncoder(follower, ch)
	// read join command
	jReq := &joinReq{}
	err := decode.Decode(jReq)
	if err != nil {
		lg.Printf("Error serving follower at %s : %s", follower.RemoteAddr(), err)
		return
	}
	// register with leader
	leader := true
	if leader.IsLeader {
		lf := leader.raft.VerifyLeader()
		err := lf.Error()
		if err != nil {
			lg.Printf("Error while verifying leader on host %s : %s", leader.rpcLayer.Addr().String(), err)
			leader = false
		}
		peerAddr, err := net.ResolveTCPAddr(jReq.peerAddr)
		if err != nil {
			lg.Printf("Couldn't resolve pathname %s processing join from %s", jReq.peerAddr, follower.RemoteAddr().String())
			follower.Close()
			return
		}
		leader.raft.AddPeer(jReq.peerAddr)
	} else {
		leader = false
	}
	if !leader {
		// send response indicating leader is someone else, then return
		lg.Printf("Node %s not leader, refusing connection to peer %s", leader.rpcLayer.Addr().String(), jReq.peerAddr)
		leaderAddr := leader.raft.LeaderAddr()
		jResp := &joinResp{""}
		if leaderAddr != nil {
			jResp.leaderHost = leaderAddr.String()
		}
		encode.Encode(jResp)
		follower.Close()
		return
	}
	// read commands
	cmdReq := &raft.Log{}
	cmdResp := &commandResp{}
	err = nil
	futures := make(chan raft.ApplyFuture, 16)
	defer func() {
		// die
		follower.Close()
		close(futures)
	}()
	go sendResponses(futures, lg, encode, follower)
	for {
		err = decode.Decode(cmdReq)
		if err != nil {
			lg.Printf("Error reading command from node %s : '%s', closing conn", follower.RemoteAddr().String(), err.Error())
			return
		}
		// exec with leader
		future := leader.raft.Apply(cmdReq.Data, nil)
		futures <- future
	}
}

// runs alongside serveFollower to send actual responses
func sendResponses(futures chan raft.ApplyFuture, lg *log.Logger, e *codec.Encoder, conn net.Conn) {
	resp := &cmdResp{}
	for f := range futures {
		err := f.Error()
		resp.Err = err
		err = e.Encode(resp)
		if err != nil {
			lg.Printf("Error writing response %s to host %s : %s", resp, conn.RemoteAddr().String(), err)
			conn.Close()
			return
		}
	}
}

type joinReq struct {
	peerAddr string
}

type joinResp struct {
	leaderHost string // "" is success, "addr:port" of leader if we're not leader
}

type commandReq struct {
	Reqno      uint64
	OriginAddr string
	Cmd        string
	Args       [][]byte
}

// commandResp has an error if the leader had a non-command-caused error
// while applying the command.  can be ErrNotLeader.
type commandResp struct {
	Err error
}

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

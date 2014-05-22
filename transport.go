package flotilla

import (
	"bytes"
	"github.com/ugorji/go/codec"
	"net"
	"sync"
)

// flotilla client connection
type connToLeader struct {
	c       net.Conn
	l       *sync.Mutex
	pending map[uint64]chan commandResp
}

// joins the raft leader and sets up infrastructure for
// processing commands
// can return ErrNotLeader
func (c *connToLeader) joinLeader(conn net.Conn) (*connToLeader, error) {
	// send join command
	return nil, nil
}

// sends the command for remote execution.
// returns nil channel and error if we couldn't communicate with leader or
// already submitted the provided reqno.
// commandResp will have non-nil error if we successfully transmitted to leader but
// the leader had a non-command-returned error such as i/o or ErrNotLeader.
// Command-returned errors are ignored, we'll get them when command is replicated to local statemachine.
func (c *connToLeader) forwardCommand(cmd commandReq) (<-chan commandResp, error) {
	// check we don't already have commandResp
	// send command
	// register callback
	// return
	return nil, nil
}

type connFromFollower struct {
	c            net.Conn
	followerAddr string
}

// reads a command as bytes
func (c *connFromFollower) readCommand() ([]byte, error) {
	return nil, nil
}

// invoke this if we're no longer leader
func (c *connFromFollower) Close() error {
	return c.c.Close()
}

type joinReq struct {
	peerAddr string
}

type joinResp struct {
	err error
}

type commandReq struct {
	Reqno      uint64
	Cmd        string
	OriginAddr string
	Args       [][]byte
}

//func (r *commandReq) MarshalBinary() ([]byte, error) {
//	buf, err := encodeMsgPack(r)
//	if err != nil {
//		return nil, err
//	}
//	return buf.Bytes(), nil
//}

//func (r *commandReq) UnmarshalBinary(data []byte) error {
//	return decodeMsgPack(data, r)
//}

// commandResp has an error if the leader had a non-command-caused error
// while applying the command.  can be ErrNotLeader.
type commandResp struct {
	Reqno uint64
	Err   error
}

//func (r *commandResp) MarshalBinary() ([]byte, error) {
//	buf, err := encodeMsgPack(r)
//	if err != nil {
//		return nil, err
//	}
//	return buf.Bytes(), nil
//}

//func (r *commandResp) UnmarshalBinary(data []byte) error {
//	return decodeMsgPack(data, r)
//}

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

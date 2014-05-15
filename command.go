package flotilla

import (
	"bytes"
	"github.com/jbooth/flotilla/raft"
	"github.com/ugorji/go/codec"
)

// exported for visibility to encoding utils
type RaftLeaderCommandReq struct {
	Reqno      uint64
	Cmd        string
	OriginAddr string
	Args       [][]byte
}

func (r *RaftLeaderCommandReq) MarshalBinary() ([]byte, error) {
	buf, err := encodeMsgPack(r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type RaftLeaderCommandResp struct {
	Reqno uint64
	Err   error
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

const (
	E_NOTLEADER = "NOT_LEADER"
)

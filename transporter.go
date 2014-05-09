package flotilla

import (
	"bufio"
	"encoding/binary"
	"net"
	"sync"
)

type opReq struct {
	op    int
	reqno uint64
	b     *ByteArgs
}

func (r opReq) ToByteArgs() *ByteArgs {
	out := &ByteArgs{make([][]byte, len(r.b.B)+2)}
	out.B[0] = make([]byte, 4, 4)

	binary.LittleEndian.PutUint32(out.B[0], uint32(r.op))
	binary.LittleEndian.PutUint64(out.B[1], r.reqno)
	for i := 0; i < len(r.b.B); i++ {
		out.B[i+2] = r.b.B[i]
	}
	return out
}

func (r opReq) FromByteArgs(b *ByteArgs) {
	r.op = int(binary.LittleEndian.Uint32(b.B[0]))
	r.reqno = binary.LittleEndian.Uint64(b.B[1])
	r.b = &ByteArgs{b.B[2:]}
}

type rawConn struct {
	mLock   *sync.Mutex // guards response map
	cLock   *sync.Mutex // guards conn
	c       net.Conn
	out     *bufio.Writer
	reqId   uint64
	pending map[uint64]chan Result
}

func (r *rawConn) req(req opReq) <-chan Result {
	resp := make(chan Result, 1)
	r.mLock.Lock()
	r.reqId++

	return resp
}

func (r *rawConn) readResponses() {
	//in := bufio.NewReaderSize(r.c, 64*1024)
}

type NetTransporter struct {
	dial func(connectStr string) (net.Conn, error)
}

//// Parts from this transporter were heavily influenced by Peter Bougon's
//// raft implementation: https://github.com/peterbourgon/raft

////------------------------------------------------------------------------------
////
//// Typedefs
////
////------------------------------------------------------------------------------

//// An HTTPTransporter is a default transport layer used to communicate between
//// multiple servers.
//type HTTPTransporter struct {
//	DisableKeepAlives    bool
//	prefix               string
//	appendEntriesPath    string
//	requestVotePath      string
//	snapshotPath         string
//	snapshotRecoveryPath string
//	httpClient           http.Client
//	Transport            *http.Transport
//}

//type HTTPMuxer interface {
//	HandleFunc(string, func(http.ResponseWriter, *http.Request))
//}

////------------------------------------------------------------------------------
////
//// Constructor
////
////------------------------------------------------------------------------------

//// Creates a new HTTP transporter with the given path prefix.
//func NewHTTPTransporter(prefix string, timeout time.Duration) *HTTPTransporter {
//	t := &HTTPTransporter{
//		DisableKeepAlives:    false,
//		prefix:               prefix,
//		appendEntriesPath:    joinPath(prefix, "/appendEntries"),
//		requestVotePath:      joinPath(prefix, "/requestVote"),
//		snapshotPath:         joinPath(prefix, "/snapshot"),
//		snapshotRecoveryPath: joinPath(prefix, "/snapshotRecovery"),
//		Transport:            &http.Transport{DisableKeepAlives: false},
//	}
//	t.httpClient.Transport = t.Transport
//	t.Transport.ResponseHeaderTimeout = timeout
//	return t
//}

////------------------------------------------------------------------------------
////
//// Accessors
////
////------------------------------------------------------------------------------

//// Retrieves the path prefix used by the transporter.
//func (t *HTTPTransporter) Prefix() string {
//	return t.prefix
//}

//// Retrieves the AppendEntries path.
//func (t *HTTPTransporter) AppendEntriesPath() string {
//	return t.appendEntriesPath
//}

//// Retrieves the RequestVote path.
//func (t *HTTPTransporter) RequestVotePath() string {
//	return t.requestVotePath
//}

//// Retrieves the Snapshot path.
//func (t *HTTPTransporter) SnapshotPath() string {
//	return t.snapshotPath
//}

//// Retrieves the SnapshotRecovery path.
//func (t *HTTPTransporter) SnapshotRecoveryPath() string {
//	return t.snapshotRecoveryPath
//}

////------------------------------------------------------------------------------
////
//// Methods
////
////------------------------------------------------------------------------------

////--------------------------------------
//// Installation
////--------------------------------------

//// Applies Raft routes to an HTTP router for a given server.
//func (t *HTTPTransporter) Install(server Server, mux HTTPMuxer) {
//	mux.HandleFunc(t.AppendEntriesPath(), t.appendEntriesHandler(server))
//	mux.HandleFunc(t.RequestVotePath(), t.requestVoteHandler(server))
//	mux.HandleFunc(t.SnapshotPath(), t.snapshotHandler(server))
//	mux.HandleFunc(t.SnapshotRecoveryPath(), t.snapshotRecoveryHandler(server))
//}

////--------------------------------------
//// Outgoing
////--------------------------------------

//// Sends an AppendEntries RPC to a peer.
//func (t *HTTPTransporter) SendAppendEntriesRequest(server Server, peer *Peer, req *raft.AppendEntriesRequest) *raft.AppendEntriesResponse {
//	var b bytes.Buffer
//	if _, err := req.Encode(&b); err != nil {
//		traceln("transporter.ae.encoding.error:", err)
//		return nil
//	}

//	url := joinPath(peer.ConnectionString, t.AppendEntriesPath())
//	traceln(server.Name(), "POST", url)

//	httpResp, err := t.httpClient.Post(url, "application/protobuf", &b)
//	if httpResp == nil || err != nil {
//		traceln("transporter.ae.response.error:", err)
//		return nil
//	}
//	defer httpResp.Body.Close()

//	resp := &AppendEntriesResponse{}
//	if _, err = resp.Decode(httpResp.Body); err != nil && err != io.EOF {
//		traceln("transporter.ae.decoding.error:", err)
//		return nil
//	}

//	return resp
//}

//// Sends a RequestVote RPC to a peer.
//func (t *HTTPTransporter) SendVoteRequest(server Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
//	var b bytes.Buffer
//	if _, err := req.Encode(&b); err != nil {
//		traceln("transporter.rv.encoding.error:", err)
//		return nil
//	}

//	url := fmt.Sprintf("%s%s", peer.ConnectionString, t.RequestVotePath())
//	traceln(server.Name(), "POST", url)

//	httpResp, err := t.httpClient.Post(url, "application/protobuf", &b)
//	if httpResp == nil || err != nil {
//		traceln("transporter.rv.response.error:", err)
//		return nil
//	}
//	defer httpResp.Body.Close()

//	resp := &RequestVoteResponse{}
//	if _, err = resp.Decode(httpResp.Body); err != nil && err != io.EOF {
//		traceln("transporter.rv.decoding.error:", err)
//		return nil
//	}

//	return resp
//}

//func joinPath(connectionString, thePath string) string {
//	u, err := url.Parse(connectionString)
//	if err != nil {
//		panic(err)
//	}
//	u.Path = path.Join(u.Path, thePath)
//	return u.String()
//}

//// Sends a SnapshotRequest RPC to a peer.
//func (t *HTTPTransporter) SendSnapshotRequest(server Server, peer *Peer, req *SnapshotRequest) *SnapshotResponse {
//	var b bytes.Buffer
//	if _, err := req.Encode(&b); err != nil {
//		traceln("transporter.rv.encoding.error:", err)
//		return nil
//	}

//	url := joinPath(peer.ConnectionString, t.snapshotPath)
//	traceln(server.Name(), "POST", url)

//	httpResp, err := t.httpClient.Post(url, "application/protobuf", &b)
//	if httpResp == nil || err != nil {
//		traceln("transporter.rv.response.error:", err)
//		return nil
//	}
//	defer httpResp.Body.Close()

//	resp := &SnapshotResponse{}
//	if _, err = resp.Decode(httpResp.Body); err != nil && err != io.EOF {
//		traceln("transporter.rv.decoding.error:", err)
//		return nil
//	}

//	return resp
//}

//// Sends a SnapshotRequest RPC to a peer.
//func (t *HTTPTransporter) SendSnapshotRecoveryRequest(server Server, peer *Peer, req *SnapshotRecoveryRequestHeader, body StateMachineIo) *SnapshotRecoveryResponse {

//	url := joinPath(peer.ConnectionString, t.snapshotRecoveryPath)
//	traceln(server.Name(), "POST", url)

//	// spin off writer function
//	pRead, pWrite := io.Pipe()
//	writeErr := make(chan error, 1)
//	go func() {
//		defer pWrite.Close()
//		if _, err := req.Encode(pWrite); err != nil {
//			writeErr <- fmt.Errorf("transporter.rv.encoding.error:%s", err)
//			return
//		}
//		if _, err := body.WriteSnapshot(pWrite); err != nil {
//			writeErr <- fmt.Errorf("err opening StateMachine snapshot:%s", err)
//			return
//		}
//		writeErr <- nil
//		return
//	}()

//	// invoke http request and get our response
//	httpResp, err := t.httpClient.Post(url, "application/protobuf", pRead)
//	if httpResp == nil || err != nil {
//		traceln("transporter.rv.response.error:", err)
//		return nil
//	}
//	defer httpResp.Body.Close()
//	err = <-writeErr
//	if err != nil {
//		traceln("transporter.rv.write.err:", err)
//		return nil
//	}
//	resp := &SnapshotRecoveryResponse{}
//	if _, err = resp.Decode(httpResp.Body); err != nil && err != io.EOF {
//		traceln("transporter.rv.decoding.error:", err)
//		return nil
//	}

//	return resp
//}

////--------------------------------------
//// Incoming
////--------------------------------------

//// Handles incoming AppendEntries requests.
//func (t *HTTPTransporter) appendEntriesHandler(server Server) http.HandlerFunc {
//	return func(w http.ResponseWriter, r *http.Request) {
//		traceln(server.Name(), "RECV /appendEntries")

//		req := &AppendEntriesRequest{}
//		if _, err := req.Decode(r.Body); err != nil {
//			http.Error(w, "", http.StatusBadRequest)
//			return
//		}

//		resp := server.AppendEntries(req)
//		if resp == nil {
//			http.Error(w, "Failed creating response.", http.StatusInternalServerError)
//			return
//		}
//		if _, err := resp.Encode(w); err != nil {
//			http.Error(w, "", http.StatusInternalServerError)
//			return
//		}
//	}
//}

//// Handles incoming RequestVote requests.
//func (t *HTTPTransporter) requestVoteHandler(server Server) http.HandlerFunc {
//	return func(w http.ResponseWriter, r *http.Request) {
//		traceln(server.Name(), "RECV /requestVote")

//		req := &RequestVoteRequest{}
//		if _, err := req.Decode(r.Body); err != nil {
//			http.Error(w, "", http.StatusBadRequest)
//			return
//		}

//		resp := server.RequestVote(req)
//		if resp == nil {
//			http.Error(w, "Failed creating response.", http.StatusInternalServerError)
//			return
//		}
//		if _, err := resp.Encode(w); err != nil {
//			http.Error(w, "", http.StatusInternalServerError)
//			return
//		}
//	}
//}

//// Handles incoming Snapshot requests.
//func (t *HTTPTransporter) snapshotHandler(server Server) http.HandlerFunc {
//	return func(w http.ResponseWriter, r *http.Request) {
//		traceln(server.Name(), "RECV /snapshot")

//		req := &SnapshotRequest{}
//		if _, err := req.Decode(r.Body); err != nil {
//			http.Error(w, "", http.StatusBadRequest)
//			return
//		}

//		resp := server.RequestSnapshot(req)
//		if resp == nil {
//			http.Error(w, "Failed creating response.", http.StatusInternalServerError)
//			return
//		}
//		if _, err := resp.Encode(w); err != nil {
//			http.Error(w, "", http.StatusInternalServerError)
//			return
//		}
//	}
//}

//// Handles incoming SnapshotRecovery requests.
//func (t *HTTPTransporter) snapshotRecoveryHandler(server Server) http.HandlerFunc {
//	return func(w http.ResponseWriter, r *http.Request) {
//		traceln(server.Name(), "RECV /snapshotRecovery")

//		req := &SnapshotRecoveryRequestHeader{}
//		if _, err := req.Decode(r.Body); err != nil {
//			http.Error(w, "", http.StatusBadRequest)
//			return
//		}

//		resp := server.SnapshotRecoveryRequest(req, r.Body)
//		if resp == nil {
//			http.Error(w, "Failed creating response.", http.StatusInternalServerError)
//			return
//		}
//		if _, err := resp.Encode(w); err != nil {
//			http.Error(w, "", http.StatusInternalServerError)
//			return
//		}
//	}
//}

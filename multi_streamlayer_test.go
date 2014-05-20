package flotilla

import (
	"log"
	"net"
	"testing"
)

func TestMultiStream(t *testing.T) {

	testLog := log.New(os.Stderr, "", log.LstdFlags)
	listen, err := net.ListenTCP("0.0.0.0:1103")
	if err != nil {
		t.Fatal(err)
	}
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:1103")
	if err != nil {
		t.Fatal(err)
	}
	// 2 channels: 0 and 1
	streamLayers, err := NewMultiStream(
		listen,
		defaultDialer,
		addr,
		testLog,
		0,
		1)
	if err != nil {
		t.Fatal(err)
	}
	// start an echo server on each one
	go echoServer(streamLayers[0], 0, testLog)
	go echoServer(streamLayers[1], 1, testLog)
	// dial each one
	connZero, err := streamLayers[0].Dial(addr, time.Second*1)
	connOne, err := streamLayers[1].Dial(addr, time.Second*1)
	// confirm each conn goes to the correct server
	reqBytes := make([]byte, 1)
	reqBytes[0] = 5
	connZero.Write(reqBytes)
	reqBytes[1] = 7
	connOne.Write(reqBytes)
	respBytes := make([]byte, 2)
	connZero.Read(respBytes)
	// should be 5,0
	if respBytes[0] != 5 || respBytes[1] != 0 {
		t.Fatalf("Expected 5,0 from connZero, got %d,%d", respBytes[0], respBytes[1])
	}
	connOne.Read(respBytes)
	if respBytes[0] != 7 || respBytes[1] != 1 {
		t.Fatalf("Expected 7,1 from connZero, got %d,%d", respBytes[0], respBytes[1])
	}
}

// for every byte sent to us, sends back 2 bytes:  original sent and our code
func echoServer(l net.Listener, myCode byte, lg log.Logger) {
	for {
		conn, err := l.Accept()
		if err != nil {
			lg.Fatalf("Error accepting for code %d : %s", myCode, err)
		}
		go func() {
			for {
				req := make([]byte, 1)
				_, err := conn.Read(req)
				if err != nil {
					lg.Fatalf("Error reading bytes from conn for code %d : %s", myCode, err)
				}
				resp := make([]byte, 2)
				resp[0] = req[0]
				resp[1] = myCode
				_, err := conn.Write(resp)
				if err != nil {
					lg.Fatalf("Error reading bytes from conn for code %d : %s", myCode, err)
				}
			}
		}()
	}
}

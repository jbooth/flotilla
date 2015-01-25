package flotilla

import (
	"fmt"
	mdb "github.com/jbooth/gomdb"
	"os"
	"testing"
)

func TestServer(t *testing.T) {

	// set up cluster of 3
	servers := make([]DefaultOpsDB, 3)
	addrs := []string{"127.0.0.1:1203", "127.0.0.1:1204", "127.0.0.1:1205"}
	dataDirs := make([]string, 3)
	waitingUp := make([]chan error, 3)
	var err error

	cmds := defaultCommands()
	cmds["err"] = alwaysReturnsError
	cmds["get"] = get

	for i := 0; i < 3; i++ {
		dataDirs[i] = os.TempDir() + fmt.Sprintf("/flot_test_%d", i)
		chkPanic(os.RemoveAll(dataDirs[i]))
		chkPanic(os.MkdirAll(dataDirs[i], os.FileMode(0777)))
		// start first server with cluster of 1 so it elects self
		waitingUp[i] = make(chan error)
		go func(j int) {
			fmt.Printf("Initializing server %d\n", j)
			servers[j], err = NewDefaultDB(
				addrs,
				dataDirs[j],
				addrs[j],
				cmds,
			)
			waitingUp[j] <- err
		}(i)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, waiter := range waitingUp {
		err = <-waiter
		if err != nil {
			t.Fatal(err)
		}
	}
	fmt.Printf("All servers up\n")
	// figure out which is leader
	leaderIdx := 0
	for i := 0; i < 3; i++ {
		fmt.Printf("Checking if server %d is leader \n", i)
		if servers[i].IsLeader() {
			leaderIdx = i
			fmt.Printf("Leader is server %d\n", leaderIdx)
		}
	}

	leader := servers[leaderIdx]
	notLeaders := make([]DefaultOpsDB, 0, 0)
	for idx, db := range servers {
		if idx != leaderIdx {
			notLeaders = append(notLeaders, db)
		}
	}
	dbName := "test"
	// execute command from leader, test results
	fmt.Println("Testing put to leader")

	res := <-leader.Put(dbName, []byte("testKey1"), []byte("testVal1"))
	if res.Err != nil {
		t.Fatal(err)
	}
	// transactional read
	res = <-leader.Command("get", [][]byte{[]byte(dbName), []byte("testKey1")})
	if res.Err != nil {
		t.Fatal(err)
	}
	if !bytesEqual(res.Response, []byte("testVal1")) {
		t.Fatal(fmt.Errorf("Bytes not equal coming back from server, expected testVal1 got %s", string(res.Response)))
	}

	// fast path read
	reader, err := leader.Read()
	if err != nil {
		t.Fatal(err)
	}
	dbi, err := reader.DBIOpen(&dbName, 0)
	if err != nil {
		t.Fatal(err)
	}
	val, err := reader.Get(dbi, []byte("testKey1"))
	if string(val) != "testVal1" {
		t.Fatalf("Expected val testVal1 for key testKey1!  Got %s", string(val))
	}
	reader.Abort()
	// execute from follower, test results on leader & follower
	fmt.Println("Testing put to follower")
	res = <-notLeaders[0].Put(dbName, []byte("testKey1"), []byte("testVal1"))
	if res.Err != nil {
		t.Fatal(err)
	}
	fmt.Println("Put to follower succeeded")
	reader, err = leader.Read()
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = reader.DBIOpen(&dbName, 0)
	if err != nil {
		t.Fatal(err)
	}
	val, err = reader.Get(dbi, []byte("testKey1"))
	if string(val) != "testVal1" {
		t.Fatalf("Expected val testVal1 for key testKey1!  Got %s", string(val))
	}
	reader.Abort()
	reader, err = notLeaders[0].Read()
	if err != nil {
		t.Fatal(err)
	}
	dbi, err = reader.DBIOpen(&dbName, 0)
	if err != nil {
		t.Fatal(err)
	}
	val, err = reader.Get(dbi, []byte("testKey1"))
	if string(val) != "testVal1" {
		t.Fatalf("Expected val testVal1 for key testKey1!  Got %s", string(val))
	}
	reader.Abort()
	// check a non-registered custom command, should get error
	res = <-servers[0].Command("NOTHERE", [][]byte{})
	if res.Err == nil {
		t.Fatalf("Should have gotten error for invalid command!")
	}
	// check a command returning error, shoult get it
	res = <-servers[0].Command("err", [][]byte{})
	if res.Err == nil {
		t.Fatalf("Should have gotten error for command with error")
	}

	// kill leader, check for new leader

	// execute some more commands

	// bring node 0 back up

	// execute commands from node 0 (now follower)
}

var alwaysReturnsError Command = func(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	return nil, fmt.Errorf("LOL error")
}

// transactional get for testing get commands over the wire
// arg0: dbName
// arg1: key
func get(args [][]byte, txn *mdb.Txn) ([]byte, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("Get needs 2 arguments!  Got %d args", len(args))
	}
	dbName := string(args[0])
	dbi, err := txn.DBIOpen(&dbName, mdb.CREATE) // create if not exists
	if err != nil {
		txn.Abort()
		return nil, err
	}
	ret, err := txn.Get(dbi, args[1])
	if err != nil {
		txn.Abort()
		return nil, err
	}
	txn.Abort()
	return ret, err
}

func chkPanic(err error) {
	if err != nil {
		panic(err)
	}
}

//func bytesEqual(a []byte, b []byte) bool {
//	// both nil is false
//	if (a == nil || b == nil || len(a) != len(b)) {
//		return false
//	}
//	for idx,v := range a {
//		if b[idx] != v {
//			return false
//		}
//	}
//	return true
//}

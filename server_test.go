package flotilla

import (
	"fmt"
	"os"
	"testing"
)

func TestServer(t *testing.T) {

	// set up cluster of 3
	servers := make([]DB, 3)
	addrs := []string{"127.0.0.1:1203", "127.0.0.1:1204", "127.0.0.1:1205"}
	dataDirs := make([]string, 3)
	waitingUp := make([]chan error, 3)
	var err error

	for i := 0; i < 3; i++ {
		dataDirs[i] = os.TempDir() + fmt.Sprintf("/flot_test_%d", i)
		chkPanic(os.RemoveAll(dataDirs[i]))
		chkPanic(os.MkdirAll(dataDirs[i], 0777))
		// start first server with cluster of 1 so it elects self
		waitingUp[i] = make(chan error)
		go func(j int) {
			fmt.Printf("Initializing server %d\n", j)
			servers[j], err = NewDB(
				addrs,
				dataDirs[j],
				addrs[j],
				defaultCommands(),
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
	notLeaders := make([]DB, 0, 0)
	for idx, db := range servers {
		if idx != leaderIdx {
			notLeaders = append(notLeaders, db)
		}
	}
	// execute command from leader, test results
	dbName := "test"
	res := <-leader.Put(dbName, []byte("testKey1"), []byte("testVal1"))
	if res.Err != nil {
		t.Fatal(err)
	}
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
	// execute from follower, test results on leader & follower

	// kill leader, check for new leader

	// execute some more commands

	// bring node 0 back up

	// execute commands from node 0 (now follower)
}

func chkPanic(err error) {
	if err != nil {
		panic(err)
	}
}

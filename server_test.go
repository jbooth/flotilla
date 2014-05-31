package flotilla

import (
	"fmt"
	"os"
	"testing"
)

func TestServer(t *testing.T) {

	// set up cluster of 3
	servers := make([]DB, 3)
	addrs := []string{"127.0.0.1:1103", "127.0.0.1:1104", "127.0.0.1:1105"}
	dataDirs := make([]string, 3)
	var err error
	for i := 0; i < 3; i++ {
		dataDirs[i] = os.TempDir() + fmt.Sprintf("/flot_test_%d", i)
		chkPanic(os.RemoveAll(dataDirs[i]))
		chkPanic(os.MkdirAll(dataDirs[i], 0777))
		servers[i], err = NewDB(
			addrs,
			dataDirs[i],
			addrs[i],
			defaultCommands(),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	// figure out which is leader
	fmt.Printf("All servers up\n")
	leaderIdx := 0
	for i := 0; i < 3; i++ {
		if servers[i].IsLeader() {
			leaderIdx = i
			fmt.Printf("Leader is server %d\n", leaderIdx)
		}
	}
	// execute command from leader, test results

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

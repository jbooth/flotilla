package flotilla

import (
	"os"
	"testing"
)

func TestServer(t *testing.T) {

	// set up cluster of 3
	servers := make([]DB, 3)
	addrs := []string{"127.0.0.1:1103", "127.0.0.1:1104", "127.0.0.1:1105"}
	dataDirs := make([]string, 3)
	for i := 0; i < 3; i++ {
		dataDirs[i] = os.TempDir() + fmt.Sprintf("/flot_test_%d", i)
		chkPanic(os.RemoveAll(dataDirs[i]))
		chkPanic(os.MkdirAll(dataDirs[i]))
	}
	var err error
	// make a leader
	servers[0], err = NewDB(
		make([]string, 0),
		dataDir,
		addrs[0],
		ops)
	if err != nil {
		panic(err)
	}
	// make 2 followers
	for i := 1; i < len(servers); i++ {
		servers[i], err = NewDB(
			peers,
			dataDirs[i],
			addrs[i],
			ops,
		)
		if err != nil {
			panic(err)
		}
	}
	// wait all up

	// confirm they all think index 0 is leader

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

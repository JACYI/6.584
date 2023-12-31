package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	PENDING = iota + 1
	RUNNING
	FAILED
	FINISHED
)

type ExampleArgs struct {
	MapOrReduce bool
	Id          int
}

type ExampleReply struct {
	Task    *Task
	Status  int
	NReduce int
	NMap    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

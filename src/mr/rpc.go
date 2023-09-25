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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	nReduce int
}

type ExampleReply struct {
	task       *Task
	status     int
	expireTime uint8
	seqNum     int
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

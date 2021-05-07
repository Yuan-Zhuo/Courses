package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type EmptyArgs struct {
}

type EmptyReply struct {
}

// GetMapTask
type GetMapTaskReply struct {
	FinishFlag  bool
	SuccessFlag bool
	FileName    string
	NumReduce   int
	IdxMap      int
	VerNum      int
}

// CommitMapTask
type CommitMapTaskArgs struct {
	IdxMap                    int
	VerNum                    int
	IntermediateFilenameSlice []string
}

// GetReduceTask
type GetReduceTaskReply struct {
	StartFlag                 bool
	FinishFlag                bool
	SuccessFlag               bool
	IdxReduce                 int
	VerNum                    int
	IntermediateFilenameSlice []string
}

// CommitReduceTask
type CommitReduceTaskArgs struct {
	IdxReduce    int
	VerNum       int
	out_filename string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

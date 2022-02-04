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

type WorkerState uint8

const (
	WSFree   WorkerState = iota // i'm doing nothing
	WSMap                       // i'm running a map task
	WSReduce                    // i'm running a reduce task
)

type WorkerCommandType uint8

const (
	WCNothing WorkerCommandType = iota // do nothing
	WCMap                              // run a map task
	WCReduce                           // run a reduce task
	WCExit                             // exit, the job is done
)

type WorkerCommand struct {
	CommandType WorkerCommandType
	Arguments   []string
}

type PingArgs struct {
	ClientId  uint32
	State     WorkerState
	WorkingOn string
}
type PingReply struct {
	CallBackInMilli int
	Command         WorkerCommand
}

func (s WorkerState) String() string {
	switch s {
	case WSFree:
		return "Free"
	case WSMap:
		return "Map"
	case WSReduce:
		return "Reduce"
	default:
		return "Unknown"
	}
}

func (c WorkerCommandType) String() string {
	switch c {
	case WCNothing:
		return "DoNothing"
	case WCExit:
		return "Exit"
	case WCMap:
		return "Map"
	case WCReduce:
		return "Reduce"
	default:
		return "Unknown"
	}
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

package mr

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type InternalState struct {
	clientId  uint32
	workingOn string
	state     WorkerState
	mut       sync.Mutex
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var clientId uint32
	binary.Read(rand.Reader, binary.LittleEndian, &clientId)

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// setup internal state
	state := InternalState{
		clientId:  clientId,
		state:     WSFree,
		workingOn: "",
		mut:       sync.Mutex{},
	}

	done := make(chan bool)

	workQueue := make(chan WorkerCommand)

	// start the ping timer
	go pingAtInterval(&state, workQueue, done)

	// start the worker
	go monitorWorkQueue(&state, workQueue)

	// block here until we're done
	_ = <-done

	log.Println(clientId, "Exiting")
	// todo: flush stdout

}

func monitorWorkQueue(state *InternalState, workQueue <-chan WorkerCommand) {
	for command := range workQueue {
		state.mut.Lock()

		freeToDoWork := state.state == WSFree
		state.mut.Unlock()

		if !freeToDoWork {
			continue
		}

		if command.CommandType == WCMap {
			// validate command.arguments

			if command.Arguments == nil {
				command.Arguments = []string{}
			}

			if len(command.Arguments) != 2 {
				log.Println(state.clientId, "received command", command.CommandType, "but not enough arguments were sent:", command.Arguments)
				log.Println(state.clientId, "skipping command")
				continue
			}
			state.mut.Lock()
			state.state = WSMap
			state.workingOn = command.Arguments[0]
			state.mut.Unlock()

			go doMapTask(state)

		} else if command.CommandType == WCReduce {
			// validate command.arguments

			if command.Arguments == nil {
				command.Arguments = []string{}
			}

			if len(command.Arguments) != 2 {
				log.Println(state.clientId, "received command", command.CommandType, "but not enough arguments were sent:", command.Arguments)
				log.Println(state.clientId, "skipping command")
				continue
			}

			state.mut.Lock()
			state.state = WSReduce
			state.workingOn = command.Arguments[0]
			state.mut.Unlock()

			go doReduceTask(state)

		} else {
			log.Println(state.clientId, "received command", command.CommandType, "on the workQueue. Not handling!")
		}

	}
}

func doMapTask(state *InternalState) {

}

func doReduceTask(state *InternalState) {

}

func pingAtInterval(state *InternalState, workQueue chan<- WorkerCommand, done chan<- bool) {
	retries := 0
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case _ = <-ticker.C:
			ticker.Stop()
			args := PingArgs{
				ClientId:  state.clientId,
				State:     state.state,
				WorkingOn: state.workingOn,
			}
			reply := PingReply{}
			if call("Coordinator.Ping", &args, &reply) {
				retries = 0
				log.Println(state.clientId, ": Ping Pong with reply", reply)

				if reply.Command.CommandType == WCExit {
					done <- true
					return
				}

				if reply.Command.CommandType == WCMap || reply.Command.CommandType == WCReduce {
					workQueue <- reply.Command
				}

				ticker.Reset(time.Millisecond * time.Duration(reply.CallBackInMilli))
			} else {
				retries += 1
				if retries == 4 {
					done <- true
					return
				}
				ticker.Reset(time.Second)
			}
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

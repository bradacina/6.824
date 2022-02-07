package mr

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strconv"
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
	clientId uint32
	state    WorkerState
	mut      sync.Mutex
	output   []string
	workDone chan bool
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

	workDone := make(chan bool)

	// setup internal state
	state := InternalState{
		clientId: clientId,
		state:    WSFree,
		mut:      sync.Mutex{},
		workDone: workDone,
	}

	done := make(chan bool)

	workQueue := make(chan WorkerCommand)

	// start the ping timer
	go pingAtInterval(&state, workQueue, workDone, done)

	// start the worker
	go monitorWorkQueue(&state, workQueue)

	// block here until we're done
	_ = <-done

	log.Println(clientId, "Exiting")
	// todo: flush stdout

}

func monitorWorkQueue(s *InternalState, workQueue <-chan WorkerCommand) {
	for command := range workQueue {
		s.mut.Lock()
		canDoWork := s.state == WSFree || s.state == WSFinished
		s.mut.Unlock()

		if !canDoWork {
			continue
		}

		if command.CommandType == WCMap {
			// validate command.arguments

			if command.Arguments == nil {
				command.Arguments = []string{} // this should not happen
			}

			if len(command.Arguments) != 3 {
				log.Println(s.clientId, "received command", command.CommandType, "but not enough arguments were sent:", command.Arguments)
				log.Println(s.clientId, "skipping command")
				continue
			}
			s.mut.Lock()
			s.state = WSMap
			s.mut.Unlock()

			go doMapTask(s, command.Arguments[0], command.Arguments[1], command.Arguments[2])

		} else if command.CommandType == WCReduce {
			// validate command.arguments

			if command.Arguments == nil {
				command.Arguments = []string{}
			}

			if len(command.Arguments) == 0 {
				log.Println(s.clientId, "received command", command.CommandType, "but no arguments were sent")
				log.Println(s.clientId, "skipping command")
				continue
			}

			s.mut.Lock()
			s.state = WSReduce
			s.mut.Unlock()

			go doReduceTask(s, command.Arguments)

		} else {
			log.Println(s.clientId, "received command", command.CommandType, "on the workQueue. Not handling!")
		}

	}
}

func doMapTask(s *InternalState, inputFile string, outputPrefix string, nReduceString string) {
	s.mut.Lock()
	clientId := s.clientId
	s.state = WSMap
	s.output = nil
	s.mut.Unlock() // unlock the mutex so Ping can run while we do the work

	nReduce, err := strconv.Atoi(nReduceString)
	if err != nil {
		log.Println("[ERROR]", clientId, "Cannot convert number of reduce tasks", nReduceString, "to integer")
		os.Exit(1)
	}

	log.Println(clientId, "Starting Map task on input file", inputFile, "with number of reduce outputs", nReduce)

	// todo: do the work

	output := make([]string, 0, nReduce)

	for i := 0; i < nReduce; i++ {
		output = append(output, outputPrefix+strconv.Itoa(i))
	}

	// pretend we do work for 4 seconds
	wait := time.NewTimer(4 * time.Second)
	<-wait.C

	log.Println(clientId, "Map task finished")

	s.mut.Lock()
	defer s.mut.Unlock()
	s.output = output
	s.state = WSFinished
	s.workDone <- true
}

func printS(s *InternalState) {
	log.Println(s.clientId, " State:", s.state)
}

func doReduceTask(s *InternalState, arguments []string) {
	s.mut.Lock()
	clientId := s.clientId
	s.state = WSReduce
	s.output = nil
	s.mut.Unlock() // unlock the mutex so Ping can run while we do the work

	log.Println(clientId, "Starting Reduce task on input", arguments)

	// pretend we do work for 4 seconds
	wait := time.NewTimer(4 * time.Second)
	<-wait.C

	log.Println(clientId, "Reduce task finished")

	s.mut.Lock()
	defer s.mut.Unlock()
	s.state = WSFinished
	s.workDone <- true
}

func pingAtInterval(s *InternalState, workQueue chan<- WorkerCommand, workDone <-chan bool, done chan<- bool) {
	retries := 0
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case _ = <-workDone:
			//fallthrough
			ticker.Reset(time.Millisecond)
			continue
		case _ = <-ticker.C:
			ticker.Stop()

			s.mut.Lock()
			workerState := s.state
			output := s.output
			s.mut.Unlock()

			args := PingArgs{
				ClientId: s.clientId,
				State:    workerState,
				Output:   output,
			}
			reply := PingReply{}
			if call("Coordinator.Ping", &args, &reply) {
				retries = 0 // reset number of retries since we had a succesful Ping

				log.Println(s.clientId, ": Ping Pong with reply", reply)

				// prevent WSFinished from being sent twice
				if workerState == WSFinished {
					s.mut.Lock()
					s.state = WSFree
					s.mut.Unlock()
				}

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

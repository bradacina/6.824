package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type TaskType uint8

const (
	TTMap TaskType = iota
	TTReduce
)

type Task struct {
	taskType     TaskType
	done         bool
	input        string
	outputPrefix string
}

type ConnectedWorker struct {
	clientId string
	lastPing time.Time
	task     *Task
}

type Coordinator struct {
	jobDone     int32
	workers     map[string]ConnectedWorker // map[clientId, ConnectedWorker]
	tasks       []Task
	nReduce     int
	reducePhase bool // we're either in the Map phase or Reduce phase
}

func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	reply.CallBackInMilli = 3000
	log.Println("Coordinator: Received Ping from ", args.ClientId, "and is currently performing", args.State)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if atomic.LoadInt32(&c.jobDone) == 1 {

		ret = true
		log.Println("Coordinator exiting")
		// todo: flush stdout
	}

	return ret
}

/// waits for the
func doneTimerFunc(ch <-chan time.Time, done *int32) {
	_ = <-ch

	_ = atomic.SwapInt32(done, 1)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// create the map tasks
	tasks := make([]Task, len(files))

	for i, f := range files {
		tasks = append(tasks, Task{
			taskType:     TTMap,
			input:        f,
			done:         false,
			outputPrefix: f + "-" + strconv.Itoa(i) + "-",
		})
	}

	c := Coordinator{
		jobDone:     0,
		tasks:       tasks,
		workers:     make(map[string]ConnectedWorker, 10),
		nReduce:     nReduce,
		reducePhase: false,
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	c.server()
	go doneTimerFunc(time.NewTimer(time.Second*10).C, &c.jobDone)
	return &c
}

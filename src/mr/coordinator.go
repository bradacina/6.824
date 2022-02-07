package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TaskType uint8

const (
	TTMap TaskType = iota
	TTReduce
)

type Phase uint8

const (
	PMap Phase = iota
	PReduce
	PExit
)

type Task struct {
	id           int
	taskType     TaskType
	input        []string
	outputPrefix string
	output       []string
}

type ConnectedWorker struct {
	clientId uint32
	lastPing time.Time
	taskId   int
}

type Coordinator struct {
	jobDone         int32
	mut             sync.Mutex
	workers         map[uint32]*ConnectedWorker // map<clientId, ConnectedWorker>
	tasksTodo       []*Task
	tasksInProgress []*Task
	tasksDone       []*Task
	nReduce         int // number of reduce tasks
	numInputFiles   int
	phase           Phase
}

var lastTaskId = 0

func newTaskId() int {
	lastTaskId += 1
	return lastTaskId
}

var pingIntervalMillis = 500
var pingTimeoutMillis = 3000

//
// Receive Ping from workers, and respond back with commands
//
func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	reply.CallBackInMilli = pingIntervalMillis
	log.Println("Coordinator: Received Ping from ", args.ClientId, "and is currently performing", args.State)

	// find the worker in our list
	c.mut.Lock()
	defer c.mut.Unlock()

	// if we're in the Exit phase, we gracefully tell the worker to exit and ignore everything else
	if c.phase == PExit {
		reply.Command = WorkerCommand{CommandType: WCExit}
		return nil
	}

	client, found := c.workers[args.ClientId]
	if !found {
		client = &ConnectedWorker{
			clientId: args.ClientId,
		}

		c.workers[args.ClientId] = client
	}

	// update last ping time
	// todo: add timer to detect when a client has not sent us a ping after a while
	client.lastPing = time.Now()

	if args.State == WSMap || args.State == WSReduce {
		// this worker is busy with some task, let it continue working
		return nil
	}

	// ensure the worker is not newly connected and is already reporting that it finished some task
	// it might be a worker that timed out and is now reconnecting, so its task has been probably assigned
	// to another worker. we cannot trust what this worker is telling us
	if args.State == WSFinished && found {
		// this worker has just finished with its current task

		var task *Task
		taskIndex := -1
		// find the task that's assigned to the client in the tasksInProgress
		for i, t := range c.tasksInProgress {
			if t.id == client.taskId {
				task = t
				taskIndex = i
				break
			}
		}

		// move the finished task from tasksInProgress to tasksDone
		if taskIndex == -1 {
			// maybe this task has been already finished by another worker (this task was run in parallel by two or more workers)
			// todo: check that the task can be found in c.tasksDone, otherwise report [ERROR]
			log.Println("[WARNING]", client.clientId, "has finished a task but I cannot find it in the tasksInProgress")
		} else {

			if task.taskType == TTMap {
				// save the output file paths into the task's output
				if len(args.Output) != c.numInputFiles {
					log.Println("[WARNING] Coordinator: Client", client.clientId,
						"did not return enough Outputs when finishing Map task. Expected:", c.numInputFiles,
						", received", len(args.Output))
				}

				task.output = append(task.output, args.Output...)
				log.Println("Coordinator: Client", client.clientId, "finished a Map task and returned", args.Output)
			}
			c.tasksInProgress = append(c.tasksInProgress[:taskIndex], c.tasksInProgress[taskIndex+1:]...)
			c.tasksDone = append(c.tasksDone, task)
		}

		client.taskId = 0 // cleanup our record of what task was the client working on

		// if there's no other task in progress, maybe we should move from Map phase to Reduce phase
		if len(c.tasksInProgress) == 0 && c.phase == PMap {
			c.initReducePhase()
		}

		// if there are no more tasks in progress and no tasks todo, the map/reduce job is finished
		if len(c.tasksInProgress) == 0 && len(c.tasksTodo) == 0 {
			if c.phase != PReduce {
				log.Println("[ERROR]", client.clientId, "length of tasksInProgress and tasksTodo is zero but the phase is not PReduce, it is", c.phase)
			}

			c.phase = PExit
			timeToExit := 4 // seconds
			log.Println("Coordinator: Reduce phase completed. Starting Exit phase and will shutdown in", timeToExit, "seconds")
			go doneTimerFunc(time.NewTimer(time.Second*time.Duration(timeToExit)).C, &c.jobDone)

			reply.Command = WorkerCommand{CommandType: WCExit}
			// todo: start a shut down timer to give time to other workers to connect and receive the WCExit command
			return nil
		}
	}

	// check for worker corruption
	if args.State == WSFree && client.taskId != 0 {
		log.Println("Coordinator: client ", client.clientId, "is reporting that it's Free, but it was assigned task", client.taskId)
		log.Println("Coordinator: forcing client", client.clientId, "to Exit")

		// move task back to tasksToDo
		for i, t := range c.tasksInProgress {
			if t.id == client.taskId {
				c.tasksInProgress = append(c.tasksInProgress[:i], c.tasksInProgress[i+1:]...)
				c.tasksTodo = append(c.tasksTodo, t)
			}
		}
		reply.Command = WorkerCommand{CommandType: WCExit}
		return nil
	}

	// find a task to assign to this worker
	if len(c.tasksTodo) == 0 {
		reply.Command = WorkerCommand{CommandType: WCNothing}
		return nil
	}

	todo := c.tasksTodo[len(c.tasksTodo)-1]
	c.tasksTodo[len(c.tasksTodo)-1] = nil
	c.tasksTodo = c.tasksTodo[:len(c.tasksTodo)-1]
	c.tasksInProgress = append(c.tasksInProgress, todo)
	client.taskId = todo.id

	log.Println("Coordinator client", client)

	if todo.taskType == TTMap {
		reply.Command = WorkerCommand{
			CommandType: WCMap,
			Arguments:   []string{todo.input[0], todo.outputPrefix, strconv.Itoa(c.nReduce)},
		}
	} else {
		reduceArguments := make([]string, 0, len(todo.input)+1)
		reduceArguments = append(reduceArguments, todo.outputPrefix)
		reduceArguments = append(reduceArguments, todo.input...)
		reply.Command = WorkerCommand{
			CommandType: WCReduce,
			Arguments:   reduceArguments,
		}
	}

	return nil
}

//
// Generate new reduce tasks in tasksTodo and set the phase.
// ATTENTION: This method assumes you have acquired a lock on the Coordinator.mut mutex
//
func (c *Coordinator) initReducePhase() {
	c.phase = PReduce
	log.Println("Coordinator: Map phase completed. Starting Reduce phase")
	for i := 0; i < c.nReduce; i++ {

		// each Map task has multiple outputs, one for each Reduce task that will be run over it,
		// which means we'll have a total of nReduce outputs. We need to find the output that's
		// meant for Reduce task "i"

		outputs := findOutputFromMap(i, c.tasksDone)
		if len(outputs) != c.numInputFiles {
			log.Println("Coordinator: cannot find all outputs for reduce task", i, ". Found", len(outputs), "out of", c.numInputFiles)
		}

		c.tasksTodo = append(c.tasksTodo, &Task{
			id:           newTaskId(),
			taskType:     TTReduce,
			input:        outputs,
			outputPrefix: "reduce-" + strconv.Itoa(i),
		})
	}
}

func findOutputFromMap(reduceTask int, mapTasks []*Task) []string {
	suffix := "-" + strconv.Itoa(reduceTask)

	result := make([]string, 0)

	for _, t := range mapTasks {
		if t.taskType == TTReduce {
			continue
		}

		output, found := findOutputFromMapTask(suffix, t.output)
		if !found {
			log.Println("Coordinator: cannot find output for reduce", reduceTask, "in the outputs of task", t.id)
		} else {
			result = append(result, output)
		}
	}

	return result
}

func findOutputFromMapTask(reduceTaskSuffix string, outputs []string) (string, bool) {
	for _, o := range outputs {
		if strings.HasSuffix(o, reduceTaskSuffix) {
			return o, true
		}
	}

	return "", false
}

//
//
//
func (c *Coordinator) monitorTimeouts() {
	timer := time.NewTicker(time.Second)
	for {
		select {
		case _ = <-timer.C:
			c.checkTimeouts()
		}
	}
}

func (c *Coordinator) checkTimeouts() {
	c.mut.Lock()
	defer c.mut.Unlock()
	timedOutClientIds := make([]uint32, 0)

	now := time.Now()

	for _, worker := range c.workers {
		if now.Sub(worker.lastPing) > time.Duration(pingTimeoutMillis)*time.Millisecond {
			timedOutClientIds = append(timedOutClientIds, worker.clientId)
		}
	}

	for _, clientId := range timedOutClientIds {
		worker, _ := c.workers[clientId]

		var task *Task
		var taskId = -1
		for i, t := range c.tasksInProgress {
			if t.id == worker.taskId {
				taskId = i
				task = t
			}
		}

		log.Println("Coordinator: worker", clientId, "has timedout. Moving task back to tasksTodo")

		if taskId != -1 {
			c.tasksInProgress = append(c.tasksInProgress[:taskId], c.tasksInProgress[taskId+1:]...)
			c.tasksTodo = append(c.tasksTodo, task)
		}

		// remove worker
		delete(c.workers, clientId)
	}
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

/// waits for a timer to expire before signaling the program to exit
func doneTimerFunc(ch <-chan time.Time, done *int32) {
	_ = <-ch

	log.Println("Coordinator: Max time to live reached! Exiting...")

	_ = atomic.SwapInt32(done, 1)
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// create the map tasks
	tasks := make([]*Task, 0, len(files))

	for i, f := range files {
		tasks = append(tasks, &Task{
			id:           newTaskId(),
			taskType:     TTMap,
			input:        []string{f},
			outputPrefix: f + "-" + strconv.Itoa(i) + "-",
			output:       make([]string, 0, nReduce),
		})
	}

	c := Coordinator{
		jobDone:         0,
		tasksTodo:       tasks,
		tasksInProgress: make([]*Task, 0, len(files)),
		tasksDone:       make([]*Task, 0, 2*len(files)),
		workers:         make(map[uint32]*ConnectedWorker, 10),
		nReduce:         nReduce,
		phase:           PMap,
		numInputFiles:   len(files),
	}

	log.SetFlags(log.Ltime | log.Lmicroseconds)

	c.server()
	go doneTimerFunc(time.NewTimer(time.Second*40).C, &c.jobDone)
	go c.monitorTimeouts()
	return &c
}

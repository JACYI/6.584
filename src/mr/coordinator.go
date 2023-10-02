package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	MAP_PHASE = iota + 1
	REDUCE_PHASE
	PENDING_RUNNING
	TASK_DONE
)

const TimeOut = 10 * time.Second

type CStatus int

type Task struct {
	Id          int
	Filename    string
	BeginTime   time.Time
	MapOrReduce bool // true is map, false is reduce
	SeqNum      int
}

type Coordinator struct {
	// Your definitions here.
	nReduce int
	nMap    int
	lock    sync.Mutex

	MapQueue    chan *Task
	ReduceQueue chan *Task
	runningSet  map[int]*Task

	mapDoneNum    int
	reduceDoneNum int
}

func checkForStatus(c *Coordinator) CStatus {
	if c.mapDoneNum == c.nMap && c.reduceDoneNum == c.nReduce {
		return FINISHED
	}
	if c.mapDoneNum == c.nMap {
		if c.reduceDoneNum+len(c.runningSet) == c.nReduce {
			return PENDING_RUNNING
		} else if c.reduceDoneNum == c.nReduce {
			return TASK_DONE
		}
		return REDUCE_PHASE
	}

	if c.mapDoneNum+len(c.runningSet) == c.nMap {
		return PENDING_RUNNING
	}

	return MAP_PHASE
}

/*
check the running task status and cancel it when timeout
*/
func runWatchDog(c *Coordinator, task *Task) {
	time.Sleep(TimeOut)
	c.lock.Lock()
	defer c.lock.Unlock()

	if task, ok := c.runningSet[task.Id]; ok && time.Now().Sub(task.BeginTime) > TimeOut {
		// cancel the timeout task and reput it into the task queue
		delete(c.runningSet, task.Id)
		log.Printf("[task-watch-dog] task %v timeout\n", task.SeqNum)
		if task.MapOrReduce {
			log.Printf("[task-watch-dog] task %v reput into map queue\n", task.SeqNum)
			c.MapQueue <- task
		} else {
			log.Printf("[task-watch-dog] task %v reput into reduce queue\n", task.SeqNum)
			c.ReduceQueue <- task
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *ExampleArgs, reply *ExampleReply) error {
	var task *Task
	c.lock.Lock()
	defer c.lock.Unlock()

	reply.NReduce = c.nReduce
	curStatus := checkForStatus(c)

	switch curStatus {
	case PENDING_RUNNING:
		reply.Status = PENDING
		return nil

	case TASK_DONE:
		reply.Status = FINISHED
		return nil

	case MAP_PHASE:
		task = <-c.MapQueue
		log.Printf("[coordinator] map task %v starts\n", task.SeqNum)
		break
	case REDUCE_PHASE:
		task = <-c.ReduceQueue
		log.Printf("[coordinator] reduce task %v starts\n", task.SeqNum)
		break
	}

	/* watch dog use task.Id to diff map and reduce phase */

	if _, ok := c.runningSet[task.Id]; ok {
		log.Panic("[coordinator] Task was running")
	}
	c.runningSet[task.Id] = task
	task.BeginTime = time.Now()
	reply.Task = task
	reply.Status = RUNNING
	reply.Task.MapOrReduce = curStatus == MAP_PHASE
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	/* async goroutine start watch dogs */
	go runWatchDog(c, task)

	return nil
}

func (c *Coordinator) CompleteTask(args *ExampleArgs, reply *ExampleReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.runningSet[args.Id]; ok {
		if args.MapOrReduce {
			c.mapDoneNum++
			log.Printf("[coordinator] map task %v done!", args.Id)
		} else {
			c.reduceDoneNum++
			log.Printf("[coordinator] reduce task %v done!", args.Id)
		}
		delete(c.runningSet, args.Id)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	ret := false

	if c.mapDoneNum == c.nReduce && c.reduceDoneNum == c.nReduce {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.MapQueue = make(chan *Task, len(files))
	c.ReduceQueue = make(chan *Task, nReduce)
	c.runningSet = make(map[int]*Task, nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)

	for i, file := range files {
		t := Task{}
		t.Filename = file
		t.MapOrReduce = true
		t.SeqNum = i
		t.Id = i
		// push to channel
		c.MapQueue <- &t
		log.Printf("Map Task[%s] pushed to the map queue\n", t.Filename)
	}

	for i := 0; i < nReduce; i++ {
		t := Task{}
		t.SeqNum = i
		t.Id = i + c.nMap
		t.MapOrReduce = false
		c.ReduceQueue <- &t
		log.Printf("Reduce Task[%d] pushed to the map queue\n", t.SeqNum)
	}

	c.server()
	return &c
}

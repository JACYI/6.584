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

type CStatus int

type Task struct {
	Filename    string
	BeginTime   time.Time
	MapOrReduce bool // true is map, false is reduce
	SeqNum      int
}

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	nMap        int
	lock        sync.Mutex
	MapQueue    chan *Task
	ReduceQueue chan *Task
	runningSet  map[int]bool

	mapDoneNum    int
	reduceDoneNum int
	//runningNum    int
	phase string
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

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *ExampleArgs, reply *ExampleReply) error {
	var task *Task
	c.lock.Lock()
	defer c.lock.Unlock()

	reply.NReduce = c.nReduce
	curStatus := checkForStatus(c)
	log.Printf("status: %v\n", curStatus)

	switch curStatus {
	case PENDING_RUNNING:
		reply.Status = PENDING
		log.Printf("pending\n")
		return nil

	case TASK_DONE:
		reply.Status = FINISHED
		log.Printf("all were finished\n")
		return nil

	case MAP_PHASE:
		log.Printf("map queue size: %v", len(c.MapQueue))
		task = <-c.MapQueue
		break
	case REDUCE_PHASE:
		log.Printf("reduce queue size: %v", len(c.ReduceQueue))
		task = <-c.ReduceQueue
		break
	}

	log.Printf("task file: %v\n", task.Filename)
	if c.runningSet[task.SeqNum] {
		log.Panic("Task was running")
	}
	c.runningSet[task.SeqNum] = true
	task.BeginTime = time.Time{}
	reply.Task = task
	reply.Status = RUNNING
	reply.Task.MapOrReduce = curStatus == MAP_PHASE
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	return nil
}

func (c *Coordinator) CompleteTask(args *ExampleArgs, reply *ExampleReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.runningSet[args.SeqNum] {
		if args.MapOrReduce {
			c.mapDoneNum++
			log.Printf("map task %v done!", args.SeqNum)
		} else {
			c.reduceDoneNum++
			log.Printf("reduce task %v done!", args.SeqNum)
		}
		delete(c.runningSet, args.SeqNum)
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

	c.MapQueue = make(chan *Task, nReduce)
	c.ReduceQueue = make(chan *Task, nReduce)
	c.runningSet = make(map[int]bool, nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.phase = "map"

	for i, file := range files {
		t := Task{}
		t.Filename = file
		t.MapOrReduce = true
		t.SeqNum = i
		// push to channel
		c.MapQueue <- &t
		log.Printf("Map Task[%s] pushed to the map queue\n", t.Filename)
	}

	for i := 0; i < nReduce; i++ {
		t := Task{}
		t.SeqNum = i
		t.MapOrReduce = false
		c.ReduceQueue <- &t
		log.Printf("Reduce Task[%d] pushed to the map queue\n", t.SeqNum)
	}

	c.server()
	return &c
}

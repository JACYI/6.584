package mr

import (
	"log"
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
	filename    string
	beginTime   time.Time
	mapOrReduce bool // true is map, false is reduce
	seqNum      int
}

//type TaskQueueNode struct {
//	task *Task
//	prev *TaskQueueNode
//	next *TaskQueueNode
//}
//type TaskQueue struct {
//	head  *TaskQueueNode
//	tail  *TaskQueueNode
//	mutex sync.Mutex
//	size  uint32
//}
//
//func NewNode(tsk *Task) *TaskQueueNode {
//	ret := TaskQueueNode{task: tsk}
//	return &ret
//}
//
//func (t *TaskQueue) Push(task *Task) error {
//	t.mutex.Lock()
//	defer t.mutex.Unlock()
//	if t.head == nil && t.tail == nil {
//		// initialize when insert the first node
//		t.head = NewNode(nil)
//		t.tail = t.head
//	}
//
//	newNode := NewNode(task)
//	t.head.next.prev = newNode
//	newNode.next = t.head.next
//	t.head.next = newNode
//	newNode.prev = t.head
//	t.size++
//
//	//t.mutex.Unlock()
//	return nil
//}
//
//func (t *TaskQueue) Pop() (*Task, error) {
//	t.mutex.Lock()
//	defer t.mutex.Unlock()
//
//	if t.size == 0 {
//		t.mutex.Unlock()
//		return nil, errors.New("task queue is empty")
//	}
//
//	lastNode := t.tail.prev
//	lastNode.prev.next = t.tail
//	t.tail.prev = lastNode.prev
//	t.size--
//
//	lastNode.prev = nil
//	lastNode.next = nil
//
//	return lastNode.task, nil
//}
//
//func (t *TaskQueue) Size() uint32 {
//	t.mutex.Lock()
//	defer t.mutex.Unlock()
//
//	return t.size
//}

type Coordinator struct {
	// Your definitions here.
	nReduce     int
	MapQueue    chan *Task
	ReduceQueue chan *Task
	runningSet  map[string]bool

	mapDoneNum    int
	reduceDoneNum int
	runningNum    int
}

func (c *Coordinator) checkForStatus() CStatus {
	if c.mapDoneNum == c.nReduce && c.reduceDoneNum == c.nReduce {
		return FINISHED
	}
	if c.mapDoneNum == c.nReduce {
		if c.reduceDoneNum+c.runningNum == c.nReduce {
			return PENDING_RUNNING
		} else if c.reduceDoneNum == c.nReduce {
			return TASK_DONE
		}
		return REDUCE_PHASE
	}

	if c.mapDoneNum+c.runningNum == c.nReduce {
		return PENDING_RUNNING
	}

	return MAP_PHASE
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *ExampleArgs, reply *ExampleReply) error {
	var task *Task

	curStatus := c.checkForStatus()

	switch curStatus {
	case PENDING_RUNNING:
		reply.status = PENDING
		return nil

	case TASK_DONE:
		reply.status = FINISHED
		return nil

	case MAP_PHASE:
	case REDUCE_PHASE:
		break
	}

	/* check for reduce queue */
	/* check for map queue */
	task = <-c.ReduceQueue
	if c.runningSet[task.filename] {
		log.Panic("task was running")
	}
	c.runningSet[task.filename] = true
	task.beginTime = time.Time{}
	reply.task = task
	reply.status = RUNNING
	reply.task.mapOrReduce = curStatus == MAP_PHASE

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {

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
	ret := false

	// Your code here.
	//if c.MapQueue == 0 && c.ReduceQueue.size == 0 && len(c.runningSet) == 0 {
	//	ret = true
	//}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.MapQueue = make(chan *Task, nReduce)
	c.ReduceQueue = make(chan *Task, nReduce)
	c.nReduce = nReduce
	for i, file := range files {
		t := Task{}
		t.filename = file
		t.mapOrReduce = true
		t.seqNum = i
		// push to channel
		c.MapQueue <- &t
		log.Printf("Task[%s] push to the map queue\n", t.filename)
	}

	c.server()
	return &c
}

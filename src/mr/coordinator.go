package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	PENDING = iota + 1
	RUNNING
	FAILED
	FINISHED
)

type Task struct {
	filename    string
	beginTime   time.Time
	status      int
	mapOrReduce bool // true is map, false is reduce
}

type TaskQueueNode struct {
	task *Task
	prev *TaskQueueNode
	next *TaskQueueNode
}
type TaskQueue struct {
	head     *TaskQueueNode
	tail     *TaskQueueNode
	mutex    sync.Mutex
	size     uint32
	capacity uint32
}

func NewNode(tsk *Task) *TaskQueueNode {
	ret := TaskQueueNode{task: tsk}
	return &ret
}

func (t *TaskQueue) Push(task *Task) error {
	t.mutex.Lock()
	if t.head == nil && t.tail == nil {
		// initialize when insert the first node
		t.head = NewNode(nil)
		t.tail = t.head
	}
	if t.size == t.capacity {
		t.mutex.Unlock()
		return errors.New("task queue is full")
	}

	newNode := NewNode(task)
	t.head.next.prev = newNode
	newNode.next = t.head.next
	t.head.next = newNode
	newNode.prev = t.head
	t.size++

	t.mutex.Unlock()
	return nil
}

func (t *TaskQueue) Pop() (*Task, error) {
	t.mutex.Lock()

	if t.size == 0 {
		t.mutex.Unlock()
		return nil, errors.New("task queue is empty")
	}

	lastNode := t.tail.prev
	lastNode.prev.next = t.tail
	t.tail.prev = lastNode.prev
	t.size--

	lastNode.prev = nil
	lastNode.next = nil
	t.mutex.Unlock()

	return lastNode.task, nil
}

type Coordinator struct {
	// Your definitions here.
	MapQueue    TaskQueue
	ReduceQueue TaskQueue
	runningSet  map[string]bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *ExampleArgs, reply *ExampleReply) error {
	var err error
	var task *Task

	/* check for reduce queue */
	task, err = c.ReduceQueue.Pop()
	if err == nil {
		if c.runningSet[task.filename] {
			log.Panic("task was running")
		}
		c.runningSet[task.filename] = true
		task.status = RUNNING
		task.beginTime = time.Time{}
		reply.task = task
		return nil
	}

	/* check for map queue */
	task, err = c.MapQueue.Pop()
	if err == nil {
		if c.runningSet[task.filename] {
			log.Panic("task was running")
		}
		c.runningSet[task.filename] = true
		task.status = RUNNING
		task.beginTime = time.Time{}
		reply.task = task
		return nil
	}

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

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for _, file := range files {
		t := Task{}
		t.filename = file
		t.mapOrReduce = true
		t.status = PENDING

		err := c.MapQueue.Push(&t)
		if err != nil {
			log.Panic("failed to push task to MapQueue")
		}
		fmt.Printf("Task[%s] push to the map queue\n", t.filename)
	}

	c.server()
	return &c
}

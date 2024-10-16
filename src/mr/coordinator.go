package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	ID        int
	Type      string
	File      string
	Status    TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	mapsDone    bool
	done        bool
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			ID:     i,
			Type:   "map",
			File:   file,
			Status: Idle,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			ID:     i,
			Type:   "reduce",
			Status: Idle,
		}
	}

	c.server()
	go c.checkTimeouts()

	return &c
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mapsDone {
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == Idle {
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.TaskType = "map"
				reply.TaskID = i
				reply.File = c.mapTasks[i].File
				reply.NReduce = c.nReduce
				return nil
			}
		}
		reply.TaskType = "wait"
		return nil
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].Status == Idle {
			c.reduceTasks[i].Status = InProgress
			c.reduceTasks[i].StartTime = time.Now()
			reply.TaskType = "reduce"
			reply.TaskID = i
			reply.NReduce = c.nReduce
			return nil
		}
	}

	if c.done {
		reply.TaskType = "done"
	} else {
		reply.TaskType = "wait"
	}
	return nil
}

func (c *Coordinator) TaskCompleted(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		c.mapTasks[args.TaskID].Status = Completed
		c.checkMapsDone()
	} else if args.TaskType == "reduce" {
		c.reduceTasks[args.TaskID].Status = Completed
		c.checkReducesDone()
	}

	return nil
}

func (c *Coordinator) checkMapsDone() {
	for _, task := range c.mapTasks {
		if task.Status != Completed {
			return
		}
	}

	c.mapsDone = true
}

func (c *Coordinator) checkReducesDone() {
	for _, task := range c.reduceTasks {
		if task.Status != Completed {
			return
		}
	}

	c.done = true
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

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *Coordinator) checkTimeouts() {
	for !c.Done() {
		time.Sleep(10 * time.Second)
		c.mu.Lock()
		now := time.Now()
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == InProgress && now.Sub(c.mapTasks[i].StartTime) > 10*time.Second {
				c.mapTasks[i].Status = Idle
			}
		}
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == InProgress && now.Sub(c.reduceTasks[i].StartTime) > 10*time.Second {
				c.reduceTasks[i].Status = Idle
			}
		}
		c.mu.Unlock()
	}
}

package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var taskTimeout = 10 * time.Second

type mapTask struct {
	filepath    string
	workerId    *int
	completedBy *int
	timeout     time.Time
}

type reduceTask struct {
	workerId    *int
	completedBy *int
	timeout     time.Time
}

type Coordinator struct {
	job           JobInfo
	mapTasks      []mapTask
	reduceTasks   []reduceTask
	workerCounter int
	mu            sync.Mutex
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Job = c.job
	reply.WorkerId = c.workerCounter
	c.workerCounter++
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id := range c.mapTasks {
		task := &c.mapTasks[id]
		if task.workerId == nil {
			task.workerId = &args.WorkerId
			task.timeout = time.Now().Add(taskTimeout)
			reply.Kind = TaskMap
			reply.Map.Filename = task.filepath
			reply.Map.TaskId = id
			return nil
		}
	}

	kind, reduceTask := c.getReduceTask(args)
	reply.Kind = kind
	reply.Reduce = reduceTask
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Kind == TaskMap {
		c.mapTasks[args.TaskId].completedBy = &args.WorkerId
		return nil
	}
	if args.Kind == TaskReduce {
		c.reduceTasks[args.TaskId].completedBy = &args.WorkerId
		return nil
	}
	return errors.New("Invalid task kind")
}

func (c *Coordinator) getReduceTask(args *GetTaskArgs) (TaskKind, ReduceTask) {
	if !c.allMapTasksDone() {
		return TaskWait, ReduceTask{}
	}
	for id := range c.reduceTasks {
		task := &c.reduceTasks[id]
		if task.workerId == nil {
			task.workerId = &args.WorkerId
			task.timeout = time.Now().Add(taskTimeout)
			return TaskReduce, ReduceTask{TaskId: id}
		}
	}
	if c.allReduceTasksDone() {
		return TaskExit, ReduceTask{}
	}
	return TaskWait, ReduceTask{}
}

func (c *Coordinator) allMapTasksDone() bool {
	i := 0
	for _, task := range c.mapTasks {
		if task.completedBy != nil {
			i++
		}
	}
	return i == c.job.NMap
}

func (c *Coordinator) allReduceTasksDone() bool {
	i := 0
	for _, task := range c.reduceTasks {
		if task.completedBy != nil {
			i++
		}
	}
	return i == c.job.NReduce
}

func (c *Coordinator) freeExpiredTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for id := range c.mapTasks {
		task := &c.mapTasks[id]
		if task.completedBy == nil && now.After(task.timeout) {
			task.workerId = nil
		}
	}
	for id := range c.reduceTasks {
		task := &c.reduceTasks[id]
		if task.completedBy == nil && now.After(task.timeout) {
			task.workerId = nil
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
	go func() {
		for {
			c.freeExpiredTasks()
			time.Sleep(time.Second)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.allReduceTasksDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.job.NMap = len(files)
	c.job.NReduce = nReduce
	c.workerCounter = 0
	c.mapTasks = make([]mapTask, len(files))
	for i, file := range files {
		c.mapTasks[i].filepath = file
	}
	c.reduceTasks = make([]reduceTask, nReduce)
	c.server(sockname)
	return &c
}

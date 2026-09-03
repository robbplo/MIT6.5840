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

type Coordinator struct {
	// Your definitions here.
	job                  JobInfo
	filesToMap           []string
	mapTasks             map[int]int
	reduceTasks          map[int]int
	completedMapTasks    map[int]int
	completedReduceTasks map[int]int
	workerCounter        int
	workerHeartbeats     map[int]time.Time
	workerDead           map[int]bool
	mu                   sync.Mutex
}

// TODO: unmark map jobs on worker failure
// TODO: reassign jobs after x amount of time

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Job = c.job
	reply.WorkerId = c.workerCounter
	c.workerHeartbeats[c.workerCounter] = time.Now()
	c.workerCounter++
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.workerDead[args.WorkerId] {
		reply.Kind = TaskExit
		return nil
	}
	for i := 0; i < len(c.filesToMap); i++ {
		_, ok := c.mapTasks[i]
		if !ok {
			c.mapTasks[i] = args.WorkerId
			reply.Kind = TaskMap
			reply.Map.Filename = c.filesToMap[i]
			reply.Map.TaskId = i
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
	// fmt.Printf("complete task %v\n", args.TaskId)
	if args.Kind == TaskMap {
		c.completedMapTasks[args.TaskId] = args.WorkerId
		return nil
	}
	if args.Kind == TaskReduce {
		c.completedReduceTasks[args.TaskId] = args.WorkerId
		return nil
	}
	return errors.New("Invalid task kind")
}

func (c *Coordinator) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerHeartbeats[args.WorkerId] = time.Now()

	return nil
}

func (c *Coordinator) getReduceTask(args *GetTaskArgs) (TaskKind, ReduceTask) {
	for i := 0; i < c.job.NReduce; i++ {
		_, ok := c.reduceTasks[i]
		if !ok {
			c.reduceTasks[i] = args.WorkerId
			taskId := i
			return TaskReduce, ReduceTask{TaskId: taskId}
		}
	}
	if len(c.completedReduceTasks) == c.job.NReduce {
		return TaskExit, ReduceTask{}
	}
	return TaskWait, ReduceTask{}
}

func (c *Coordinator) checkLiveness() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for workerId, timestamp := range c.workerHeartbeats {
		if now.After(timestamp.Add(time.Second * 3)) {
			c.killWorker(workerId)
		}
	}
}

func (c *Coordinator) killWorker(deadWorkerId int) {
	c.workerDead[deadWorkerId] = true
	for i, workerId := range c.completedMapTasks {
		if workerId == deadWorkerId {
			delete(c.completedMapTasks, i)
		}
	}
	for i, workerId := range c.mapTasks {
		if workerId == deadWorkerId {
			delete(c.mapTasks, i)
		}
	}
	for i, workerId := range c.reduceTasks {
		if workerId == deadWorkerId {
			delete(c.reduceTasks, i)
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
			c.checkLiveness()
			time.Sleep(time.Second)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	reduce := len(c.completedReduceTasks)
	// mapcount := len(c.completedMapTasks)
	// fmt.Printf("Map: %v tasks out of %v\n", mapcount, c.job.NMap)
	// fmt.Printf("Reduce: %v tasks out of %v\n", reduce, c.job.NReduce)
	return reduce == c.job.NReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.job.NMap = len(files)
	c.job.NReduce = nReduce
	c.filesToMap = files
	c.workerCounter = 0
	c.mapTasks = map[int]int{}
	c.reduceTasks = map[int]int{}
	c.completedMapTasks = map[int]int{}
	c.completedReduceTasks = map[int]int{}
	c.workerHeartbeats = map[int]time.Time{}
	c.workerDead = map[int]bool{}
	c.server(sockname)
	return &c
}

package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	filesToMap    []string
	reduceTasks   int
	currentTask   int
	workerCounter int
	mu            sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceTasks = c.reduceTasks
	reply.Id = c.workerCounter
	c.workerCounter++
	return nil
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentTask >= len(c.filesToMap) {
		return errors.New("No tasks remaining")
	}
	current := c.currentTask
	reply.Filename = c.filesToMap[current]
	reply.TaskId = c.currentTask
	c.currentTask++
	return nil
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
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.filesToMap = files
	c.reduceTasks = nReduce
	c.workerCounter = 0

	// Your code here.

	c.server(sockname)
	return &c
}

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
	job               JobInfo
	filesToMap        []string
	currentMapTask    int
	currentReduceTask int
	workerCounter     int
	mu                sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Job = c.job
	reply.Id = c.workerCounter
	c.workerCounter++
	return nil
}

func (c *Coordinator) GetMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentMapTask >= len(c.filesToMap) {
		return errors.New("No tasks remaining")
	}
	reply.Filename = c.filesToMap[c.currentMapTask]
	reply.TaskId = c.currentMapTask
	c.currentMapTask++
	return nil
}

func (c *Coordinator) GetReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	for c.currentMapTask < len(c.filesToMap) {
		time.Sleep(time.Millisecond * 10)
		println("waiting for map to finish")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentReduceTask >= c.job.NReduce {
		reply.HasTask = false
		return nil
	}
	reply.HasTask = true
	reply.TaskId = c.currentReduceTask
	c.currentReduceTask++
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentReduceTask >= c.job.NReduce
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

	c.server(sockname)
	return &c
}

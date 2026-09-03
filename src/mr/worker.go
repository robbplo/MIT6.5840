package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var coordSockName string // socket for coordinator

type worker struct {
	job      JobInfo
	mapf     MapF
	reducef  ReduceF
	workerId int
}

// main/mrworker.go calls this function.
func RunWorker(sockname string, mapf MapF, reducef ReduceF) {
	coordSockName = sockname

	worker, err := registerWorker(mapf, reducef)
	if err != nil {
		fmt.Printf("Failed to register worker, exiting...")
		return
	}

	for {
		taskReply, err := getTask()
		if err != nil {
			break
		}
		switch taskReply.Kind {
		case TaskMap:
			worker.runMapTask(taskReply.Map)
		case TaskReduce:
			worker.runReduceTask(taskReply.Reduce)
		case TaskWait:
			time.Sleep(time.Second)
		case TaskExit:
			return
		}
	}
}

func (w *worker) runMapTask(mapTask MapTask) error {
	file, err := os.Open(mapTask.Filename)
	if err != nil {
		return err
	}
	defer file.Close()
	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	intermediate := w.mapf(mapTask.Filename, string(bytes))
	buckets := map[int]ByKey{}
	for _, kv := range intermediate {
		intermediate_id := ihash(kv.Key) % w.job.NReduce
		bucket := buckets[intermediate_id]
		bucket = append(bucket, kv)
		buckets[intermediate_id] = bucket
	}

	for i := range w.job.NReduce {
		sort.Sort(buckets[i])
		filename := fmt.Sprintf("mr-%v-%v", mapTask.TaskId, i)
		file, err = os.Create(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range buckets[i] {
			enc.Encode(kv)
		}
	}

	_, err = completeTask(w.workerId, TaskMap, mapTask.TaskId)
	if err != nil {
		return err
	}

	return nil
}

func (w *worker) runReduceTask(task ReduceTask) {
	outfilename := fmt.Sprintf("mr-out-%v", task.TaskId)
	outfile, err := os.Create(outfilename)
	if err != nil {
		return
	}
	defer outfile.Close()
	kva := ByKey{}
	for mapId := range w.job.NMap {
		intfilename := fmt.Sprintf("mr-%v-%v", mapId, task.TaskId)
		intfile, err := os.Open(intfilename)
		if err != nil {
			return
		}
		defer intfile.Close()
		dec := json.NewDecoder(intfile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(kva)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[i].Key == kva[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.reducef(kva[i].Key, values)
		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	completeTask(w.workerId, TaskReduce, task.TaskId)
}

// Coordinator RPCs

func registerWorker(mapf MapF, reducef ReduceF) (worker, error) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	w := worker{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		return w, errors.New("Failed to register worker")
	}
	w.job = reply.Job
	w.workerId = reply.WorkerId
	w.mapf = mapf
	w.reducef = reducef
	go func() {
		for {
			heartbeat(w.workerId)
			time.Sleep(time.Second)
		}
	}()
	return w, nil
}

func getTask() (GetTaskReply, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		return reply, errors.New("Failed to get task")
	}
	return reply, nil
}

func completeTask(workerId int, kind TaskKind, taskId int) (CompleteTaskReply, error) {
	args := CompleteTaskArgs{}
	reply := CompleteTaskReply{}
	args.WorkerId = workerId
	args.Kind = kind
	args.TaskId = taskId
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		return reply, errors.New("Failed to complete task")
	}
	return reply, nil
}

func heartbeat(workerId int) error {
	args := HeartbeatArgs{}
	reply := HeartbeatReply{}
	args.WorkerId = workerId
	ok := call("Coordinator.Heartbeat", &args, &reply)
	if !ok {
		return errors.New("Heartbeat failed")
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call to %v failed err %v", os.Getpid(), rpcname, err)
	return false
}

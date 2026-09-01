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

// main/mrworker.go calls this function.
func Worker(sockname string, mapf MapF, reducef ReduceF) {
	coordSockName = sockname

	register, err := registerWorker()
	if err != nil {
		fmt.Printf("Failed to register worker, exiting...")
		os.Exit(1)
	}
	nReduce := register.Job.NReduce
	job := register.Job

	// complete map tasks
	for {
		mapTask, err := getMapTask()
		if err != nil {
			break
		}

		runMapTask(mapTask, mapf, nReduce)
	}

	// complete reduce tasks
	for {
		reduceTask, err := getReduceTask()
		if err != nil {
			fmt.Printf("Failed to get reduce task")
			os.Exit(1)
		}
		if !reduceTask.HasTask {
			break
		}
		runReduceTask(reduceTask, job, reducef)

	}
}

func runMapTask(mapTask GetMapTaskReply, mapf MapF, nReduce int) error {
	file, err := os.Open(mapTask.Filename)
	if err != nil {
		return err
	}
	defer file.Close()
	bytes, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	intermediate := mapf(mapTask.Filename, string(bytes))
	buckets := map[int]ByKey{}
	for _, kv := range intermediate {
		intermediate_id := ihash(kv.Key) % nReduce
		bucket := buckets[intermediate_id]
		bucket = append(bucket, kv)
		buckets[intermediate_id] = bucket
	}

	for i := range nReduce {
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

	return nil
}

func runReduceTask(task GetReduceTaskReply, job JobInfo, reducef ReduceF) {
	outfilename := fmt.Sprintf("mr-out-%v", task.TaskId)
	outfile, err := os.Create(outfilename)
	if err != nil {
		return
	}
	defer outfile.Close()
	kva := ByKey{}
	for mapId := range job.NMap {
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
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
}

// Coordinator RPCs

func registerWorker() (RegisterWorkerReply, error) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if !ok {
		return reply, errors.New("Failed to register worker")
	}
	return reply, nil
}

func getMapTask() (GetMapTaskReply, error) {
	args := GetMapTaskArgs{}
	reply := GetMapTaskReply{}
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if !ok {
		return reply, errors.New("No tasks remain")
	}
	return reply, nil
}

func getReduceTask() (GetReduceTaskReply, error) {
	args := GetReduceTaskArgs{}
	reply := GetReduceTaskReply{}
	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if !ok {
		return reply, errors.New("Failed to get reduce task")
	}
	return reply, nil

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
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

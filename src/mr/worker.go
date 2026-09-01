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

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf MapF, reducef ReduceF) {
	coordSockName = sockname

	register, err := registerWorker()
	if err != nil {
		fmt.Printf("Failed to register worker, exiting...")
		os.Exit(1)
	}
	nReduce := register.ReduceTasks

	for true {
		mapTask, err := getMapTask()
		if err != nil {
			break
		}

		runMapTask(mapTask, mapf, nReduce)
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
	buckets := map[int][]KeyValue{}
	for _, kv := range intermediate {
		intermediate_id := ihash(kv.Key) % nReduce
		bucket := buckets[intermediate_id]
		bucket = append(bucket, kv)
		buckets[intermediate_id] = bucket
	}

	for i := range nReduce {
		filename := fmt.Sprintf("mr-%v-%v", mapTask.TaskId, i)
		file, err = os.Create(filename)
		if err != nil {
			fmt.Printf("Could not open file %v\n", filename)
			os.Exit(1)
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range buckets[i] {
			enc.Encode(kv)
		}
	}

	return nil
}

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

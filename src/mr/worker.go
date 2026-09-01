package mr

import (
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	filename := GetMapTask()
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Could not open file %v\n", filename)
	}
	bytes, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("Could not read file %v\n", filename)
	}

	intermediate := mapf(filename, string(bytes))
	fmt.Printf("int: %v", intermediate[0])

}

func GetMapTask() string {
	args := GetMapTaskArgs{}
	reply := GetMapTaskReply{}
	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		fmt.Printf("reply.Filename %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Filename

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

package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func getTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

func doMap(mapf func(string, string) []KeyValue, task GetTaskReply) {
	callTaskCompleted("map", task.TaskID)
}

func doReduce(reducef func(string, []string) string, task GetTaskReply) {
	callTaskCompleted("reduce", task.TaskID)
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := getTask()
		fmt.Println(task)

		switch task.TaskType {
		case "map":
			doMap(mapf, task)
		case "reduce":
			doReduce(reducef, task)
		case "wait":
			time.Sleep(time.Second)
		case "done":
			return
		}
	}
}

func callTaskCompleted(taskType string, taskID int) {
	args := TaskCompletedArgs{TaskType: taskType, TaskID: taskID}
	reply := TaskCompletedReply{}
	call("Coordinator.TaskCompleted", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

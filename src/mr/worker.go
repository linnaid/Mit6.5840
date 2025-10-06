package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	// "github.com/Done-0/fuck-u-code/pkg/report"
)

var workerID int

//
// Map functions return a slice of KeyValue.
// 储存中间结果的 key 和 value
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
// 将输入的字符串转化为一个整数
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 向 coordinator 请求任务
// func ()

//
// main/mrworker.go calls this function.
// 主逻辑
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		
	// Your worker implementation here.
	workerID = 0
	for {
		args := WorkerArgs{
			WorkerID: workerID,
		}
		reply := WorkerReply{}

		ok := call("Coordinator.AssignTask", &args, &reply)
		if workerID == 0 {
			workerID = reply.WorkerID
		}
		if !ok {
			log.Fatalf("Worker %d: RPC failed", workerID)
		}

		switch reply.Task {
		case "map":
			content, err := os.ReadFile(reply.Filename)
			if err != nil {
				log.Fatalf("Worker %d: cannot read %v", workerID, reply.Filename)
			}
			kv_one := mapf(reply.Filename, string(content))
			intermediates := make([][]KeyValue, reply.NReduce)
			for _, kv := range kv_one {
				r := ihash(kv.Key) % reply.NReduce
				intermediates[r] = append(intermediates[r], kv)
			}

			// 写中间文件
			for r, kvs := range intermediates {
				wname := fmt.Sprintf("mr-%d-%d", reply.TaskID, r)
				wfile, _ := os.Create(wname)
				enc := json.NewEncoder(wfile)
				for _, kv := range kvs {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Worker %d: cannot write intermediate file", workerID)
					}
				}
				wfile.Close()
			}

			// 上报任务已完成
			reportArgs := WorkerArgs{
				WorkerID: workerID,
				TaskType: "map",
				Finished: true,
				TaskID: reply.TaskID,
			}
			reportReply := WorkerReply{}
			call("Coordinator.ReportTask", &reportArgs, &reportReply)
			
		case "reduce":
			// 整合 map 结果
			intermediate := []KeyValue{}
			for r := 0; r < reply.NMap; r++ {
				rname := fmt.Sprintf("mr-%d-%d", r, reply.TaskID)
				file, err := os.Open(rname)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					err := dec.Decode(&kv)
					if err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			// 排序
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})
			
			// 写入文件
			wname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			wfile, _ := os.Create(wname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(wfile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			wfile.Close()

			// 上传完成
			reportArgs := WorkerArgs{
				WorkerID: workerID,
				TaskType: "reduce",
				TaskID: reply.TaskID,
				Finished: true,
			}
			reportReply := WorkerReply{}
			call("Coordinator.ReportTask", &reportArgs, &reportReply)
 		case "wait":
			time.Sleep(time.Second)
		case "exit":
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

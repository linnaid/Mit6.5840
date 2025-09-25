package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

///////////////////////////////////////////////////////////////
// 以下时补充的数据结构：

// 展示任务状态的枚举
type TaskStatus int

const (
	Idle TaskStatus = iota // 空闲
	InProgress             // 正在执行中
	Completed			   // 执行完毕
)

// 单个任务的描述
type Task struct {
	Filename string	  // map任务的输入文件名
	TaskType string	  // map/reduce 任务
	Status TaskStatus // 当前任务的状态
	WorkerID int  	  // 执行任务的 workerID
	StartTime time.Time // 任务开始的时间戳
}

// 协调者的核心数据结构
type Coordinator struct {
	// Your definitions here.
	MapTasks []Task     // map 任务列表
	ReduceTasks []Task  // reduce 任务列表
	NReduce int  		// reduce 数量
	mu sync.Mutex		// 互斥锁，保护 多RPC 并发时数据访问安全
}
///////////////////////////////////////////////////////


// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// 更改
	ret := true

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}

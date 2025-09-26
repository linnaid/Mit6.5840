package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Worker 请求任务或上报任务完成
type WorkerArgs struct {
	WorkerID int;

	Finished bool  // 是否完成上一个任务
	TaskType string 
	TaskID int
}

type WorkerReply struct {
	WorkerID int
	
	TaskType string  // 任务类型
	TaskID int   // 任务编号
	FileName string  // map任务的输入文件名
	NReduce int   // Reduce数量
	NMap int 
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

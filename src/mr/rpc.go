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

// Worker 请求任务
type TaskRequestArgs struct {
	WorkerID int;
}

type TaskRequestReply struct {
	TaskType string  // 任务类型
	TaskID int   // 任务编号
	FileName string  // map任务的输入文件名
	NReduce int   // Reduce数量
	NMap int 
}


// Worker 完成任务后上报结果
type TaskCompleteArgs struct {
	WorkerID int   // 完成任务的Worker
	TaskType string    // 任务类型
	TaskID int   // 任务编号
}

type TaskCompleteReply struct {
	Ack bool   // 是否继续请求新任务
}



type RequestArgs struct {
	WorkerID int;
}

type RequestReply struct {
	WorkerID int;
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

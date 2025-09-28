package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

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
	nextWorkerID int    // 分配ID
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
// 以下时实现的 RPC Hander(请求任务/等待任务完成)
// 写的时候拼写成了 AssingTask, 导致调用失败，以后可不能犯这种拼写错误！！！                                                                                              )
func (c *Coordinator) AssignTask(args *WorkerArgs, reply *WorkerReply) error {

	// 分配ID(条件：无ID时)
	if args.WorkerID == 0 {
		c.nextWorkerID++
		reply.WorkerID = c.nextWorkerID
	} else {
		reply.WorkerID = c.nextWorkerID
	}

	// 检查超时
	c.mu.Lock()
	for i := range c.MapTasks {
		t := &c.MapTasks[i]

		if t.Status == InProgress && time.Since(t.StartTime) > 10*time.Second {
			t.Status = Idle
			t.WorkerID = 0
			log.Printf("MapTask %s (map) timed out and is reset to Idle", t.Filename)
		}
	}
	for i := range c.ReduceTasks {
		t := &c.ReduceTasks[i]

		if t.Status == InProgress && time.Since(t.StartTime) > 10*time.Second {
			t.Status = Idle
			t.WorkerID = 0
			log.Printf("MapTask %s (map) timed out and is reset to Idle", t.Filename)
		}
	}
	c.mu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	allMapDone := true
	for i := range c.MapTasks {
		t := &c.MapTasks[i]

		if t.Status == Idle {
			t.Status = InProgress
			t.StartTime = time.Now()

			reply.Task = "map"
			reply.Filename = t.Filename
			reply.TaskID = i
			reply.NReduce = c.NReduce
			return nil
		}

		if t.Status != Completed {
			allMapDone = false
		}
	}

	if allMapDone {
		for i := range c.ReduceTasks {
			t := &c.ReduceTasks[i]
			if t.Status == Idle {
				t.Status = InProgress
				t.StartTime = time.Now()

				reply.Task = "reduce"
				reply.NReduce = c.NReduce
				reply.TaskID = i
				return nil
			}

			if t.Status != Completed {
				return nil
			}
		}

		reply.Task = "exit"
		return nil
	}

	reply.Task = "wait"
	return nil
}

// 上报任务是否完成(更新任务状态)
func (c *Coordinator) ReportTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []Task
	if args.TaskType == "map" {
		tasks = c.MapTasks
	} else {
		tasks = c.ReduceTasks
	}

	t := &tasks[args.TaskID]
	if args.Finished {
		t.Status = Completed
	} else {
		t.Status = Idle
		t.WorkerID = 0
	}

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
// 是否所有任务都已完成
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.MapTasks {
		if t.Status != Completed {
			return false
		}
	} 

	for _, t := range c.ReduceTasks {
		if t.Status != Completed {
			return false
		}
	}

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce = nReduce,
	}

	// Your code here.
	for _, f := range files {
		c.MapTasks = append(c.MapTasks, Task{
			Filename: f,
			TaskType: "map",
			Status: Idle,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, Task{
			TaskType: "reduce",
			Status: Idle,
		})
	}

	c.server()
	return &c
}

package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

///////////////////////////////////////////////////////////////

type TaskStatus int

const (
	Idle TaskStatus = iota 
	InProgress             
	Completed			   
)

type Task struct {
	Filename string	  
	TaskType string	  
	Status TaskStatus 
	WorkerID int  	  
	StartTime time.Time 
}

type Coordinator struct {
	// Your definitions here.
	MapTasks []Task     
	ReduceTasks []Task  
	NReduce int  		
	NMap int
	mu sync.Mutex		
	nextWorkerID int    
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

func (c *Coordinator) AssignTask(args *WorkerArgs, reply *WorkerReply) error {

	if args.WorkerID == 0 {
		c.nextWorkerID++
		reply.WorkerID = c.nextWorkerID
	} else {
		reply.WorkerID = args.WorkerID
	}

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
			log.Printf("ReduceTask %d (reduce) timed out and is reset to Idle", i)
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
			reply.NMap = c.NMap
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
				reply.NMap = c.NMap
				reply.TaskID = i
				return nil
			}
		}

		for _, t := range c.ReduceTasks {
			if t.Status != Completed {
				reply.Task = "wait"
				return nil
			}
		}

		reply.Task = "exit"
		return nil
	}

	reply.Task = "wait"
	return nil
}

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
		NReduce: nReduce,
		NMap: len(files),
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

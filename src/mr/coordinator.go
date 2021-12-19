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

type Coordinator struct {
	// Your definitions here.
	mapJobDone    map[int]bool
	mapJobTime    map[int]time.Time // 任务发放时间
	reduceJobDone map[int]bool
	reduceJobTime map[int]time.Time
	files         []string
	nReduce       int
	mux           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetMapTask(args *Empty, reply *StringInt) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	// 遍历所有任务
	reply.N = -1
	for i, f := range c.files {
		if c.mapJobTime[i].IsZero() {
			// 任务发布时间为0，表示任务尚未发布
			// 任务未发放，发布给当前访问的rpc worker
			reply.S = f
			reply.N = i
			c.mapJobTime[i] = time.Now() // 任务发布时间
			break
		}
	}
	return nil
}

func (c *Coordinator) CheckMap(args *Empty, reply *Bool) error {
	c.mux.Lock()
	defer c.mux.Unlock() // 结束该方法时调用解锁
	reply.B = true
	for i := range c.files {
		if !c.mapJobDone[i] {
			// 只要有任务未完成，则将返回值置为false
			reply.B = false
			if time.Since(c.mapJobTime[i]) > 10*time.Second {
				// 如果任务超时10秒未完成，则将任务时间重置
				c.mapJobTime[i] = time.Time{}
			}
		}
		// 已完成任务不做任何处理
	}
	return nil
}

func (c *Coordinator) NotifyMapDone(args *Int, reply *Bool) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.mapJobDone[args.N] = true
	reply.B = true // 通知成功
	return nil
}

func (c *Coordinator) GetReduceCnt(args *Empty, reply *Int) error {
	reply.N = c.nReduce
	return nil
}

func (c *Coordinator) GetReduceTask(args *Empty, reply *Int) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	reply.N = -1
	// 遍历所有任务
	for i := 0; i < c.nReduce; i++ {
		if c.reduceJobTime[i].IsZero() {
			// 任务未发放，发布给当前访问的rpc worker
			reply.N = i
			c.reduceJobTime[i] = time.Now() // 任务发布时间
			break
		}
	}
	return nil
}

func (c *Coordinator) NotifyReduceDone(args *Int, reply *Bool) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.reduceJobDone[args.N] = true
	reply.B = true // 通知成功
	return nil
}

func (c *Coordinator) CheckReduce(args *Empty, reply *Bool) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	reply.B = true
	for i := 0; i < c.nReduce; i++ {
		if !c.reduceJobDone[i] {
			reply.B = false
			if time.Since(c.reduceJobTime[i]) > 10*time.Second {
				// 如果任务超时10秒未完成，则将任务时间重置
				c.reduceJobTime[i] = time.Time{}
			}
		}
	}
	return nil
}

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
	ret := true

	// Your code here.
	c.mux.Lock()
	defer c.mux.Unlock()
	for i := 0; i < c.nReduce; i++ {
		// 当所有reduce任务完成，可以认为该mr任务已完全完成
		if !c.reduceJobDone[i] {
			ret = false
			break
		}
	}
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
	c.nReduce = nReduce
	c.files = files
	// job map赋值，初始化全部为未完成
	c.mapJobDone = make(map[int]bool, 0)
	c.mapJobTime = make(map[int]time.Time, 0)
	c.reduceJobDone = make(map[int]bool, 0)
	c.reduceJobTime = make(map[int]time.Time, 0)

	c.server()
	return &c
}

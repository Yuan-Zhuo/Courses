package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// CurrenctStage
type Stage int8

const (
	StartStage  Stage = 0
	MapStage    Stage = 1
	ReduceStage Stage = 2
	FinishStage Stage = 3
)

// TaskType
const (
	TODO = iota - 2
	DONE
	DOING
)

// MaxUptime
const MaxUptime = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	mutex             sync.Mutex
	MapTask           []int
	ReduceTask        []int
	nMap              int
	nReduce           int
	inputfiles        []string
	intermediatefiles [][]string
	MapTaskAllDone    bool
	ReduceTaskAllDone bool
}

// Your code here -- RPC handlers for the worker to call.
//
// check expire
//
func (c *Coordinator) check_expire(task *int) {
	time.Sleep(MaxUptime)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if (*task) != DONE {
		*task = TODO
	}
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
// handle worker's request for map task
//
func (c *Coordinator) GetMapTaskService(args *EmptyArgs, reply *GetMapTaskReply) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = nil
	reply.FinishFlag = c.MapTaskAllDone
	if reply.FinishFlag {
		return
	}

	reply.SuccessFlag = false
	for i := 0; i < c.nMap; i++ {
		switch c.MapTask[i] {
		case TODO:
			reply.SuccessFlag = true
			reply.FileName = c.inputfiles[i]
			reply.NumReduce = c.nReduce
			reply.IdxMap = i
			vernum := rand.Int()
			c.MapTask[i] = vernum
			reply.VerNum = vernum
			go c.check_expire(&(c.MapTask[i]))
			return
		default:
			continue
		}
	}
	return
}

func (c *Coordinator) CommitMapTaskService(args *CommitMapTaskArgs, reply *EmptyReply) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = nil
	if c.MapTaskAllDone {
		log.Println(fmt.Sprintf("all done"))
		return
	}

	if args.VerNum != c.MapTask[args.IdxMap] {
		log.Println(fmt.Sprintf("vernum mismatch"))
		return
	}

	// accept
	for i := 0; i < c.nReduce; i++ {
		c.intermediatefiles[i] = append(c.intermediatefiles[i], args.IntermediateFilenameSlice[i])
	}
	c.MapTask[args.IdxMap] = DONE

	// update state
	flag := true
	for _, task := range c.MapTask {
		if task != DONE {
			flag = false
			break
		}
	}
	c.MapTaskAllDone = flag

	return
}

//
// handle worker's request for map task
//
func (c *Coordinator) GetReduceTaskService(args *EmptyArgs, reply *GetReduceTaskReply) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = nil
	reply.StartFlag = c.MapTaskAllDone
	if !reply.StartFlag {
		return
	}

	reply.FinishFlag = c.ReduceTaskAllDone
	if reply.FinishFlag {
		return
	}

	reply.SuccessFlag = false
	for i := 0; i < c.nReduce; i++ {
		switch c.ReduceTask[i] {
		case TODO:
			reply.SuccessFlag = true
			reply.IdxReduce = i
			reply.IntermediateFilenameSlice = c.intermediatefiles[i]
			vernum := rand.Int()
			c.ReduceTask[i] = vernum
			reply.VerNum = vernum
			go c.check_expire(&(c.ReduceTask[i]))
			return
		default:
			continue
		}
	}
	return
}

func (c *Coordinator) CommitReduceTaskService(args *CommitReduceTaskArgs, reply *EmptyReply) (err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err = nil
	if c.ReduceTaskAllDone {
		return
	}

	if args.VerNum != c.ReduceTask[args.IdxReduce] {
		return
	}

	// accept
	c.ReduceTask[args.IdxReduce] = DONE

	// update state
	flag := true
	for _, task := range c.ReduceTask {
		if task != DONE {
			flag = false
			break
		}
	}
	c.ReduceTaskAllDone = flag

	return
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
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ret = c.ReduceTaskAllDone

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
	// init (c *Coordinator)
	c.nMap = len(files)
	c.nReduce = nReduce
	c.MapTask = make([]int, c.nMap)
	for i := 0; i < c.nMap; i++ {
		c.MapTask[i] = TODO
	}
	c.ReduceTask = make([]int, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.ReduceTask[i] = TODO
	}
	c.inputfiles = files
	c.intermediatefiles = make([][]string, nReduce)
	c.MapTaskAllDone = false
	c.ReduceTaskAllDone = false

	c.server()
	return &c
}

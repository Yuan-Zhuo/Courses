package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// Avoid frequently ask for task while unfinished tasks are just being done
const IdleTime = 1 * time.Second

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	log.SetOutput(ioutil.Discard)
	log.Println("Map Stage")
	// Map
	for {
		get_reply := GetMapTask()

		if get_reply.FinishFlag {
			log.Println("all map tasks finished")
			break
		} else if !get_reply.SuccessFlag {
			log.Println("cannot get map task")
			time.Sleep(IdleTime)
		} else {
			intermediate_filename_slice, err := RunMap(get_reply, mapf)
			if err == nil {
				log.Println(fmt.Sprintf("commit map task: %v", get_reply.IdxMap))
				CommitMapTask(get_reply, intermediate_filename_slice)
			}
		}
	}

	log.Println("Reduce Stage")
	// Reduce
	for {
		get_reply := GetReduceTask()
		if !get_reply.StartFlag {
			log.Println("map tasks haven't finished")
			time.Sleep(IdleTime)
		} else if get_reply.FinishFlag {
			log.Println("all reduce tasks finished")
			break
		} else if !get_reply.SuccessFlag {
			log.Println("cannot get reduce task")
			time.Sleep(IdleTime)
		} else {
			out_filename, err := RunReduce(get_reply, reducef)
			if err == nil {
				log.Println(fmt.Sprintf("commit reduce task: %v", get_reply.IdxReduce))
				CommitReduceTask(get_reply, out_filename)
			}
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("---finish---\n")
}

// Map RPC
func GetMapTask() GetMapTaskReply {
	// declare an argument structure.
	get_args := EmptyArgs{}

	// declare a reply structure.
	get_reply := GetMapTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GetMapTaskService", &get_args, &get_reply)

	return get_reply
}

func CommitMapTask(get_reply GetMapTaskReply, intermediate_filename_slice *[]string) {
	// declare an argument structure.
	commit_args := CommitMapTaskArgs{}

	// fill in the argument(s).
	commit_args.IdxMap = get_reply.IdxMap
	commit_args.VerNum = get_reply.VerNum
	commit_args.IntermediateFilenameSlice = *intermediate_filename_slice

	// declare a reply structure.
	commit_reply := EmptyArgs{}

	// send the RPC request, wait for the reply.
	call("Coordinator.CommitMapTaskService", &commit_args, &commit_reply)
}

// Reduce RPC
func GetReduceTask() GetReduceTaskReply {
	// declare an argument structure.
	get_args := EmptyArgs{}

	// declare a reply structure.
	get_reply := GetReduceTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GetReduceTaskService", &get_args, &get_reply)

	return get_reply
}

func CommitReduceTask(get_reply GetReduceTaskReply, out_filename string) {
	// declare an argument structure.
	commit_args := CommitReduceTaskArgs{}

	// fill in the argument(s).
	commit_args.IdxReduce = get_reply.IdxReduce
	commit_args.VerNum = get_reply.VerNum
	commit_args.out_filename = out_filename

	// declare a reply structure.
	commit_reply := EmptyArgs{}

	// send the RPC request, wait for the reply.
	call("Coordinator.CommitReduceTaskService", &commit_args, &commit_reply)
}

//
// function func
//
// SortString
type SortString []string

func (a SortString) Len() int           { return len(a) }
func (a SortString) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortString) Less(i, j int) bool { return a[i] < a[j] }

// SortKeyValue
type SortKeyValue []KeyValue

func (a SortKeyValue) Len() int           { return len(a) }
func (a SortKeyValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortKeyValue) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map Task
func RunMap(get_reply GetMapTaskReply, mapf func(string, string) []KeyValue) (*[]string, error) {
	// Read Input File
	file, err := os.Open(get_reply.FileName)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot open %v", get_reply.FileName))
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("cannot read %v", get_reply.FileName))
	}
	file.Close()

	// Execute Map Task
	intermediate := mapf(get_reply.FileName, string(content))

	// Hash Spilt
	intermediate_slices := make([][]KeyValue, get_reply.NumReduce)
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % get_reply.NumReduce
		intermediate_slices[idx] = append(intermediate_slices[idx], kv)
	}
	intermediate_filename_slice := make([]string, 0)
	for i := 0; i < get_reply.NumReduce; i++ {
		out_filename := fmt.Sprintf("mr-%v-%v", get_reply.IdxMap, i)

		tmpfile, err := ioutil.TempFile("", "map")
		if err != nil {
			log.Println(fmt.Sprintf("cannot open tmpfile for %v", out_filename))
			i--
			continue
		}

		enc := json.NewEncoder(tmpfile)
		for _, kv := range intermediate_slices[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Println(fmt.Sprintf("error write %v", tmpfile))
				break
			}
		}
		if err != nil {
			i--
			continue
		}

		err = os.Rename(tmpfile.Name(), out_filename)
		if err != nil {
			log.Println(fmt.Sprintf("cannot rename %v", out_filename))
			i--
			continue
		}
		intermediate_filename_slice = append(intermediate_filename_slice, out_filename)
	}
	return &intermediate_filename_slice, nil
}

// Reduce Task
func RunReduce(get_reply GetReduceTaskReply, reducef func(string, []string) string) (string, error) {
	// Read From Filelist
	out_filename := fmt.Sprintf("mr-out-%v", get_reply.IdxReduce)
	intermediate := make([]KeyValue, 0)
	for _, intermediate_filename := range get_reply.IntermediateFilenameSlice {
		file, err := os.Open(intermediate_filename)
		if err != nil {
			return out_filename, errors.New(fmt.Sprintf("cannot open %v", intermediate_filename))
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Sort
	sort.Sort(SortKeyValue(intermediate))

	// Execute Reduce Task
	tmpfile, err := ioutil.TempFile("", "reduce")
	if err != nil {
		return out_filename, errors.New(fmt.Sprintf("cannot open tmpfile for %v", out_filename))
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err = os.Rename(tmpfile.Name(), out_filename)
	if err != nil {
		return out_filename, errors.New(fmt.Sprintf("cannot rename %v", out_filename))
	}
	return out_filename, nil
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

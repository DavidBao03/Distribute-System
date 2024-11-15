package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "sort"
import "strings"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Handle freduce
func HandleReduce(filenames []string, reducef func(string, []string) string) string {
	files := make([]*os.File, len(filenames))
	intermediate := []KeyValue{}

	for i := 0; i < len(filenames); i++ {
		files[i], _ = os.Open(filenames[i])
		kv := KeyValue{}
		dec := json.NewDecoder(files[i])

		for {
			if err := dec.Decode(&kv); err != nil {
				break;
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"
	index := filenames[0][strings.LastIndex(filenames[0], "-") + 1:]
	oname = oname + index
	ofile, _ := os.Create(oname)

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

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return oname
}

// Handle fmap
func HandleMap(filename string, mapf func(string, string) []KeyValue, filenum int, tasknum string) []string{
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	filenames := make([]string, filenum)
	files := make([]*os.File, filenum)

	for i := 0; i < filenum; i++ {
		oname := "mr"
		oname = oname + "-" + tasknum + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		files[i] = ofile
		filenames[i] = oname
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % filenum
		enc := json.NewEncoder(files[index])
		enc.Encode(&kv)
	}

	return filenames
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		request := GetTaskRequest{}
		reply := GetTaskResponse{}
		request.X = 1

		ok := call("Coordinator.GetTask", &request, &reply)
		if ok {
			if reply.TaskType == "map" {
				filenames := HandleMap(reply.MFile, mapf, reply.ReduceNumber, reply.TaskNum)

				report_request := ReportTaskRequest{}
				report_reply := ReportTaskResponse{}
				report_reply.X = 1

				report_request.TaskName = reply.TaskNum
				report_request.Files = filenames

				call("Coordinator.Report", &report_request, &report_reply)
			} else if reply.TaskType == "reduce" {
				HandleReduce(reply.RFiles, reducef)

				report_request := ReportTaskRequest{}
				report_reply := ReportTaskResponse{}
				report_reply.X = 1

				report_request.TaskName = reply.TaskNum
				report_request.Files = make([]string, 0)

				call("Coordinator.Report", &report_request, &report_reply)
			} else if reply.TaskType == "sleep" {
				time.Sleep(time.Millisecond * 10)
			}
		} else {
			fmt.Printf("call failed!\n")
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

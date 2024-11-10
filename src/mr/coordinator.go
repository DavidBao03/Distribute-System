package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "strings"
import "sync"
import "fmt"
import "time"

type Task struct {
	Name string
	Type string
	Status int
	MFileName string
	RFileName int
}

type Coordinator struct {
	// Your definitions here.
	// 0: not_started, 1: processing, 2: finished
	// map files
	MRecord map[string]int
	// reduce files
	RRecord map[int]int
	// intermedite files
	InterRecord map[int][]string
	// count of finished map
	Mcount int
	// count of finished reduce
	Rcount int
	// flag of if all map finished
	MapFinished bool
	// reduceNumber
	ReduceNumber int
	// record all tasks
	TaskMap map[string]*Task
	// mutex
	mutex sync.Mutex
}

var tasknum int = 0

// Your code here -- RPC handlers for the worker to call.
// Handle crash
func (c *Coordinator) HandleTimeout(taskName string) {
	time.Sleep(time.Second * 10)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if task, ok := c.TaskMap[taskName]; ok {
		task.Status = -1

		if task.Type == "map" {
			file := task.MFileName
			if c.MRecord[file] == 1 {
				c.MRecord[file] = 0
			}
		} else if task.Type == "reduce" {
			file := task.RFileName
			if c.RRecord[file] == 1 {
				c.RRecord[file] = 0
			}
		}
	}
}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	reply.RFiles = make([]string, 0)
	reply.ReduceNumber = c.ReduceNumber
	reply.TaskNum = strconv.Itoa(tasknum)
	tasknum += 1
	if c.MapFinished {
		for filenum := range c.RRecord {
			flag := c.RRecord[filenum]
			if flag == 1 || flag == 2 {
				continue
			} else {
				reply.TaskType = "reduce"
				c.RRecord[filenum] = 1
				
				for _, filename := range c.InterRecord[filenum] {
					reply.RFiles = append(reply.RFiles, filename)
				}

				task := &Task{reply.TaskNum, "reduce", 1, "", filenum}
				c.TaskMap[reply.TaskNum] = task

				go c.HandleTimeout(reply.TaskNum)
				return nil
			}
		}
		reply.TaskType = "sleep"
		return nil
	} else {
		for file, _ := range c.MRecord {
			flag := c.MRecord[file]
			if flag == 1 || flag == 2 {
				continue
			} else {
				reply.TaskType = "map"
				c.MRecord[file] = 1
				reply.MFile = file
				
				task := &Task{reply.TaskNum, "map", 1, file, -1}
				c.TaskMap[reply.TaskNum] = task

				go c.HandleTimeout(reply.TaskNum)
				return nil
			}
		}
		reply.TaskType = "sleep"
		return nil
	}
	return nil
}

func (c *Coordinator) Report(args *ReportTaskRequest, reply *ReportTaskResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if task, ok := c.TaskMap[args.TaskName]; ok {
		flag := task.Status
		if flag == -1 {
			delete(c.TaskMap, args.TaskName)
			return nil
		}

		taskType := task.Type
		if taskType == "map" {
			mapFile := task.MFileName
			c.MRecord[mapFile] = 2
			c.Mcount += 1
			if c.Mcount == len(c.MRecord) {
				c.MapFinished = true
			}

			for _, interFile := range args.Files {
				index := strings.LastIndex(interFile, "-")
				num, _ := strconv.Atoi(interFile[index + 1:])
				c.InterRecord[num] = append(c.InterRecord[num], interFile)
			}

			delete(c.TaskMap, task.Name)

			return nil
		} else if taskType == "reduce" {
			reduceFileNum := task.RFileName
			c.RRecord[reduceFileNum] = 2
			c.Rcount += 1
			delete(c.TaskMap, task.Name)
			return nil
		} else {
			fmt.Printf("%s\n", taskType)
			log.Fatal("error\n")
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
	ret := false

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	fmt.Printf("%d %d\n", c.Rcount, c.ReduceNumber)
	if c.Rcount == c.ReduceNumber {
		ret = true
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
	c.MRecord = make(map[string]int)
	c.RRecord = make(map[int]int)
	c.InterRecord = make(map[int][]string)
	c.Mcount = 0
	c.Rcount = 0
	c.MapFinished = false
	c.ReduceNumber = nReduce
	c.TaskMap = make(map[string]*Task)

	for _, file := range files {
		c.MRecord[file] = 0
	}

	for i := 0; i < nReduce; i++ {
		c.RRecord[i] = 0
	}

	c.server()
	return &c
}

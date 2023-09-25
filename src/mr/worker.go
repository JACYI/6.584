package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

const DELIMITER string = " "

type SortByKey []KeyValue

func (b SortByKey) Len() int           { return len(b) }
func (b SortByKey) Less(i, j int) bool { return b[i].Key <= b[j].Key }
func (b SortByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := ExampleArgs{}
		reply := ExampleReply{}

		for {
			CallForTask(&args, &reply)

			switch reply.status {
			case RUNNING:
				goto runTask
			case FINISHED:
				log.Printf("work exit")
				os.Exit(0)
			case PENDING:
				break
			case FAILED:
				log.Printf("job failed")
			}
		}

	runTask:
		if reply.task.mapOrReduce {
			/* Map task */
			file, err := os.Open(reply.task.filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.task.filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.task.filename)
			}

			kvs := mapf(reply.task.filename, string(content))

			outFilePathList := []*os.File{}
			jsonFileList := []*json.Encoder{}
			for i := 0; i < args.nReduce; i++ {
				outfilename := fmt.Sprintf("./mr-%v-%v.json", reply.seqNum, i)
				file, err := os.Open(outfilename)
				if err != nil {
					log.Fatalf("cannot open json file: %v", outfilename)
				}
				outFilePathList = append(outFilePathList, file)
				enc := json.NewEncoder(file)
				jsonFileList = append(jsonFileList, enc)
			}
			defer func(files []*os.File) {
				for _, file := range files {
					file.Close()
				}
			}(outFilePathList)

			// write
			for _, kv := range kvs {
				bucketId := ihash(kv.Key) % args.nReduce
				err := jsonFileList[bucketId].Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write kv:[%v:%v] to json file", kv.Key, kv.Value)
				}
			}

		} else {
			/* Reduce task */
			var intermediate []KeyValue
			// decode kvs from mr-*-Y.json
			for i := 0; i < args.nReduce; i++ {
				interFilePath := fmt.Sprintf("./mr-%v-%v.json", i, reply.seqNum)
				file, err := os.Open(interFilePath)
				defer func(file *os.File) {
					err := file.Close()
					if err != nil {

					}
				}(file)
				if err != nil {
					log.Fatalf("cannot open file %v when reduce", interFilePath)
				}

				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			// sort and gather
			sort.Sort(SortByKey(intermediate))

			outname := fmt.Sprintf("mr-out-%v", reply.seqNum)
			outfile, _ := os.Create(outname)
			defer func(f *os.File) {
				err := f.Close()
				if err != nil {

				}
			}(outfile)

			// reduce
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

				fmt.Fprintf(outfile, "%v%v%v\n", intermediate[i].Key, DELIMITER, output)

				i = j
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	//args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y \n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallForTask(args *ExampleArgs, reply *ExampleReply) {
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RequestTask", args, reply)
	if !ok {
		log.Panic("RPC call failed!")
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

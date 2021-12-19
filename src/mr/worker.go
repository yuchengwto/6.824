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
	"path/filepath"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 持续获取map task直到所有任务都完成
	for {
		// 检查map任务是否全部完成
		mpDoneArgs := Empty{}
		mpDoneReply := Bool{}
		connected := call("Coordinator.CheckMap", &mpDoneArgs, &mpDoneReply)
		if !connected {
			log.Fatal("can not connect to the Coordinator, exit this worker.")
		}
		if mpDoneReply.B {
			break
		}
		// 看来没有完成，尝试获取任务并执行
		CallMap(mapf)
		// 等待一秒

		time.Sleep(time.Second)
	}
	// 执行到这里，说明所有map任务已经完成

	// 持续获取reduce task直到所有任务都完成
	for {
		// 检查reduce任务是否全部完成
		rdDoneArgs := Empty{}
		rdDoneReply := Bool{B: false}
		call("Coordinator.CheckReduce", &rdDoneArgs, &rdDoneReply)
		if rdDoneReply.B {
			break
		}
		// 看来没有完成，尝试获取任务并执行
		CallReduce(reducef)
		// 等待一秒
		time.Sleep(time.Second)
	}
	// 执行到这里，说明所有reduce任务都已完成

}

func CallReduce(reducef func(string, []string) string) {
	// 获取reduce任务
	rdArgs := Empty{}
	rdReply := Int{}
	var connected bool

	connected = call("Coordinator.GetReduceTask", &rdArgs, &rdReply)
	if !connected {
		log.Fatal("can not connect to the Coordinator, exit this worker.")
	}
	// 执行reduce任务
	if rdReply.N >= 0 {
		pattern := fmt.Sprintf("./mr-*-%d", rdReply.N)
		matchFiles, _ := filepath.Glob(pattern)
		var kva []KeyValue
		for _, f := range matchFiles {
			file, err := os.Open(f)
			if err != nil {
				log.Fatalf("cannot open %v", f)
			}
			dec := json.NewDecoder(file)
			for {
				// 逐个json解码
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
			file.Close()
		}

		// 排序
		sort.Sort(ByKey(kva))

		// 创建reduce输出文件
		oname := fmt.Sprintf("mr-out-%d", rdReply.N)
		tmpFile, _ := ioutil.TempFile("", oname)
		defer tmpFile.Close()
		defer os.Remove(tmpFile.Name())
		// ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in kva[],
		// and print the result to mr-out-i.
		//
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)

			i = j
		}
		// 重命名临时文件
		os.Rename(tmpFile.Name(), oname)

		// 通知coor任务已完成
		rdDoneArgs := Int{N: rdReply.N}
		rdDoneReply := Bool{}
		connected = call("Coordinator.NotifyReduceDone", &rdDoneArgs, &rdDoneReply)
		if !connected {
			os.Remove(oname)
			log.Fatal("can not connect to the Coordinator, exit this worker.")
		}
	}
}

func DeleteIntermediate(mapId int) {
	// 删除该map任务对应的中间数据，如果有的话
	pattern := fmt.Sprintf("./mr-%d-*", mapId)
	matchFiles, _ := filepath.Glob(pattern)
	for _, f := range matchFiles {
		err := os.Remove(f)
		if err != nil {
			log.Fatalf("remove existing intermediate file error, file: %v", f)
		}
	}
}

func CallMap(mapf func(string, string) []KeyValue) {
	// 获取map任务
	mpArgs := Empty{}
	mpReply := StringInt{}

	var connected bool
	connected = call("Coordinator.GetMapTask", &mpArgs, &mpReply)
	if !connected {
		log.Fatal("can not connect to the Coordinator, exit this worker.")
	}

	// 执行map任务
	if mpReply.N >= 0 {
		// 获取到map任务
		file, err := os.Open(mpReply.S)
		if err != nil {
			log.Fatalf("cannot open %v", mpReply.S)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", mpReply.S)
		}
		file.Close()
		kva := mapf(mpReply.S, string(content))
		// 排序
		sort.Sort(ByKey(kva))

		// 获取reduce count
		rnArgs := Empty{}
		rnReply := Int{}
		connected = call("Coordinator.GetReduceCnt", &rnArgs, &rnReply)
		if !connected {
			log.Fatal("can not connect to the Coordinator, exit this worker.")
		}
		reduceCnt := rnReply.N

		// 删除该map任务对应的中间数据，如果有的话
		DeleteIntermediate(mpReply.N)

		// 保存键值对到文件
		for _, kv := range kva {
			// 获取reduce id
			rdId := ihash(kv.Key) % reduceCnt
			path := fmt.Sprintf("mr-%d-%d", mpReply.N, rdId)
			if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
				// 若文件不存在，则创建一个
				os.Create(path)
			}
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			if err != nil {
				log.Fatalf("cannot open %v", path)
			}
			enc := json.NewEncoder(f)
			enc.Encode(&kv)
			f.Close()
		}

		// 通知coor任务已完成
		mpDoneArgs := Int{N: mpReply.N}
		mpDoneReply := Bool{} // 通知成功或失败的回复
		connected = call("Coordinator.NotifyMapDone", &mpDoneArgs, &mpDoneReply)
		if !connected {
			DeleteIntermediate(mpReply.N)
			log.Fatal("can not connect to the Coordinator, exit this worker.")
		}
	}
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
	fmt.Printf("reply.Y %v\n", reply.Y)
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

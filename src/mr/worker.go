package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	id      int
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if ok := call("Master.RegWorker", &args, &reply); !ok {
		log.Fatal("register fail")
	}
	w.id = reply.WorkerId
}

func (w *worker) reqTask() Task {
	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}

	if ok := call("Master.GetOneTask", &args, &reply); !ok {
		os.Exit(1)
	}
	return *reply.Task
}

func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Index = t.Index
	args.Phase = t.Phase
	args.WorkerId = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &args, &reply); !ok {
		log.Printf("report task fail: %+v", args)
	}
}

func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.Filename)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}

	kva := w.mapf(t.Filename, string(contents))
	reduceInputBuckets := make([][]KeyValue, t.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % t.NReduce
		reduceInputBuckets[idx] = append(reduceInputBuckets[idx], kv)
	}

	for ridx, kvBucket := range reduceInputBuckets {
		filename := afterMapFilename(t.Index, ridx)
		file, err := os.Create(filename)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range kvBucket {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
				return
			}
		}
		if err := file.Close(); err != nil {
			w.reportTask(t, false, err)
			return
		}
	}
	w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t Task) {
	kva := make(map[string][]string)
	for idx := 0; idx < t.NMap; idx++ {
		filename := afterMapFilename(idx, t.Index)
		file, err := os.Open(filename)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := kva[kv.Key]; !ok {
				kva[kv.Key] = []string{}
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
	}

	res := []string{}
	for k, v := range kva {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(afterReduceFilename(t.Index), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
		return
	}
	w.reportTask(t, true, nil)
}

func (w *worker) doTask(t Task) {
	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase err: %v", t.Phase))
	}
}

func (w *worker) run() {
	// if a worker gets an invalid task, that's a sign for exiting
	for {
		task := w.reqTask()
		if !task.Valid {
			return
		}
		w.doTask(task)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	DPrintf("%+v", err)
	return false
}

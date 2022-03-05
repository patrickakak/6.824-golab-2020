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

const (
	TaskStatusReady = iota
	TaskStatusQueue
	TaskStatusRunning
	TaskStatusFinish
	TaskStatusErr
)

const (
	MaxTaskRunTime   = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

type Master struct {
	files       []string
	nReduce     int
	taskPhase   TaskPhase
	taskStats   []TaskStat
	mu          sync.Mutex
	done        bool
	workerIndex int
	taskCh      chan Task
}

func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Phase != m.taskPhase {
		panic("request task phase not equal")
	}

	m.taskStats[task.Index].Status = TaskStatusRunning
	m.taskStats[task.Index].WorkerId = args.WorkerId
	m.taskStats[task.Index].StartTime = time.Now()
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerIndex += 1
	reply.WorkerId = m.workerIndex
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.taskPhase != args.Phase || args.WorkerId != m.taskStats[args.Index].WorkerId {
		return nil
	}

	if args.Done {
		m.taskStats[args.Index].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Index].Status = TaskStatusErr
	}

	go m.schedule()
	return nil
}

func (m *Master) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <-m.taskCh
	reply.Task = &task

	if task.Valid {
		m.regTask(args, &task)
	}
	return nil
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) initMapTask() {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
}

func (m *Master) initReduceTask() {
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
}

func (m *Master) getTask(taskIndex int) Task {
	task := Task{
		Filename: "",
		NMap:     len(m.files),
		NReduce:  m.nReduce,
		Index:    taskIndex,
		Phase:    m.taskPhase,
		Valid:    true,
	}
	if task.Phase == MapPhase {
		task.Filename = m.files[taskIndex]
	}
	return task
}

func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}
	allFinish := true
	for index, t := range m.taskStats {
		switch t.Status {
		case TaskStatusReady, TaskStatusErr:
			allFinish = false
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if time.Since(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
		default:
			panic("task status err")
		}
	}
	if allFinish {
		if m.taskPhase == MapPhase {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.files = files
	m.taskCh = make(chan Task, nReduce)

	m.initMapTask()
	go m.tickSchedule()
	m.server()
	return &m
}

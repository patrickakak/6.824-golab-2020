package mr

import (
	"fmt"
	"log"
)

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
)
const Debug = false

func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format+"\n", v...)
	}
}

type Task struct {
	Phase    TaskPhase
	Filename string
	NMap     int
	NReduce  int
	Index    int  // task index depends on the task phase
	Valid    bool // worker should exit when valid is false
}

func afterMapFilename(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func afterReduceFilename(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

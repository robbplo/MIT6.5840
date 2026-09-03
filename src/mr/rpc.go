package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type JobInfo struct {
	NMap    int
	NReduce int
}

type RegisterWorkerArgs struct{}

type RegisterWorkerReply struct {
	WorkerId    int
	ReduceTasks int
	Job         JobInfo
}

type TaskKind uint8

const (
	TaskWait TaskKind = iota
	TaskMap
	TaskReduce
	TaskExit
)

type MapTask struct {
	Filename string
	TaskId   int
}

type ReduceTask struct {
	TaskId int
}

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	Kind   TaskKind
	Map    MapTask
	Reduce ReduceTask
}

type CompleteTaskArgs struct {
	Kind     TaskKind
	WorkerId int
	TaskId   int
}

type CompleteTaskReply struct{}

type HeartbeatArgs struct {
	WorkerId int
}
type HeartbeatReply struct { }

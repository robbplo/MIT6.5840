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

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	Id          int
	ReduceTasks int
	Job         JobInfo
}

type GetMapTaskArgs struct {
}

type GetMapTaskReply struct {
	Filename string
	TaskId   int
}

type GetReduceTaskArgs struct {
}

type GetReduceTaskReply struct {
	HasTask bool
	TaskId  int
}

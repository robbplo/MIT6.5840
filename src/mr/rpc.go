package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	Id          int
	ReduceTasks int
}

type GetMapTaskArgs struct {
}

type GetMapTaskReply struct {
	Filename string
	TaskId   int
}

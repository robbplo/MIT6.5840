package raft

import (
	"reflect"
	"testing"
)

// func TestUnitHandleAppendRequest(t *testing.T) {
// 	t.Run("handles out-of-order", func(t *testing.T) {
// 		rf := &Raft{
// 			currentTerm:   1,
// 			votedFor:      -1,
// 			role:          follower,
// 			log:           []entry{{Term: 0}},
// 			electionTimer: time.NewTimer(time.Hour),
// 		}
// 		t.Cleanup(func() { rf.electionTimer.Stop() })
//
// 		replies := make(chan *AppendEntriesReply, 2)
// 		rf.handleAppendRequest(appendRequest{
// 			args: AppendEntriesArgs{
// 				Entries:      []entry{{1, 1}, {1, 2}},
// 				Term:         1,
// 				PrevLogIndex: 0,
// 				PrevLogTerm:  0,
// 			},
// 			reply: replies,
// 		})
// 		rf.handleAppendRequest(appendRequest{
// 			args: AppendEntriesArgs{
// 				Entries:      []entry{{1, 1}},
// 				Term:         1,
// 				PrevLogIndex: 0,
// 				PrevLogTerm:  0,
// 			},
// 			reply: replies,
// 		})
//
// 		for range 2 {
// 			select {
// 			case reply := <-replies:
// 				if !reply.Success {
// 					t.Fatal("expected append request to succeed")
// 				}
// 				if reply.Term != rf.currentTerm {
// 					t.Errorf("reply term = %d, want %d", reply.Term, rf.currentTerm)
// 				}
// 			default:
// 				t.Fatal("expected a reply")
// 			}
// 		}
//
// 		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}}
// 		if !reflect.DeepEqual(rf.log, wantLog) {
// 			// t.Errorf("log = %v, want %v", rf.log, wantLog)
// 		}
// 	})
// }

func TestUnitClearLogAfter(t *testing.T) {
	t.Run("clears log after 'x' exclusive without snapshot", func(t *testing.T) {
		rf := &Raft{
			log:      []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshot: snapshot{LastIndex: 0},
		}
		rf.clearLogAfter(2)
		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}}

		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})

	t.Run("clears log after 'x' exclusive with snapshot", func(t *testing.T) {
		rf := &Raft{
			log:      []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshot: snapshot{LastIndex: 2},
		}
		rf.clearLogAfter(4)
		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}}

		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}

		rf.clearLogAfter(2)
		wantLog = []entry{{0, nil}}

		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})
}

func TestUnitClearLogThrough(t *testing.T) {
	t.Run("clears log until 'x' inclusive without snapshot", func(t *testing.T) {
		rf := &Raft{
			log:      []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshot: snapshot{LastIndex: 0},
		}
		rf.clearLogThrough(2)
		wantLog := []entry{{0, nil}, {1, 3}}

		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})

	t.Run("clears log until 'x' inclusive with snapshot", func(t *testing.T) {
		rf := &Raft{
			log:      []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshot: snapshot{LastIndex: 2},
		}
		rf.clearLogThrough(2)
		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}}

		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}

		rf.clearLogThrough(4)
		wantLog = []entry{{0, nil}, {1, 3}}

		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})
}

package raft

import (
	"reflect"
	"testing"
	"time"
)

func TestUnitHandleAppendRequest(t *testing.T) {
	t.Run("accepts a heartbeat with a matching previous log", func(t *testing.T) {
		rf := &Raft{
			currentTerm:   1,
			votedFor:      -1,
			role:          follower,
			log:           []entry{{Term: 0}},
			electionTimer: time.NewTimer(time.Hour),
		}
		t.Cleanup(func() { rf.electionTimer.Stop() })

		replies := make(chan *AppendEntriesReply, 2)
		rf.handleAppendRequest(appendRequest{
			args: AppendEntriesArgs{
				Entries:      []entry{{1, 1}, {1, 2}},
				Term:         1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
			},
			reply: replies,
		})
		rf.handleAppendRequest(appendRequest{
			args: AppendEntriesArgs{
				Entries:      []entry{{1, 1}},
				Term:         1,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
			},
			reply: replies,
		})

		for range 2 {
			select {
			case reply := <-replies:
				if !reply.Success {
					t.Fatal("expected append request to succeed")
				}
				if reply.Term != rf.currentTerm {
					t.Errorf("reply term = %d, want %d", reply.Term, rf.currentTerm)
				}
			default:
				t.Fatal("expected a reply")
			}
		}

		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}}
		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})
}

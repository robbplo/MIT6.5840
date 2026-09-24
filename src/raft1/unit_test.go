package raft

import (
	"reflect"
	"testing"
)

func TestUnitSetLogEntries(t *testing.T) {
	testCases := []struct {
		description   string
		snapshotIndex logIndex
		log           []entry
		setEntries    []entry
		setIndex      logIndex
		want          []entry
	}{
		{
			description:   "appends when own log is empty",
			snapshotIndex: 0,
			log:           []entry{{0, nil}},
			setEntries:    []entry{{1, 1}, {1, 2}},
			setIndex:      1,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}},
		},
		{
			description:   "appends when own log is empty (snapshot)",
			snapshotIndex: 9,
			log:           []entry{{0, nil}},
			setEntries:    []entry{{1, 1}, {1, 2}},
			setIndex:      10,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}},
		},
		{
			description:   "overwrites existing logs with same term and preserves remaining logs",
			snapshotIndex: 0,
			log:           []entry{{0, nil}, {1, 0}, {1, 0}, {1, 0}},
			setEntries:    []entry{{1, 1}, {1, 2}},
			setIndex:      1,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}, {1, 0}},
		},
		{
			description:   "overwrites existing logs with same term and preserves remaining logs (snapshot)",
			snapshotIndex: 34,
			log:           []entry{{0, nil}, {1, 0}, {1, 0}, {1, 0}},
			setEntries:    []entry{{1, 1}, {1, 2}},
			setIndex:      35,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}, {1, 0}},
		},
		{
			description:   "overwrites existing logs with same term and appends",
			snapshotIndex: 0,
			log:           []entry{{0, nil}, {1, 0}},
			setEntries:    []entry{{1, 1}, {1, 2}},
			setIndex:      1,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}},
		},
		{
			description:   "appends logs",
			snapshotIndex: 0,
			log:           []entry{{0, nil}, {1, 0}},
			setEntries:    []entry{{1, 1}, {1, 2}},
			setIndex:      2,
			want:          []entry{{0, nil}, {1, 0}, {1, 1}, {1, 2}},
		},
		{
			description:   "overwrites logs from other term",
			snapshotIndex: 0,
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {2, 3}},
			setEntries:    []entry{{3, 1}, {3, 2}, {3, 3}},
			setIndex:      1,
			want:          []entry{{0, nil}, {3, 1}, {3, 2}, {3, 3}},
		},
		{
			description:   "clears logs after non-matching term",
			snapshotIndex: 0,
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {2, 3}, {2, 4}, {2, 5}},
			setEntries:    []entry{{1, 1}, {1, 2}, {3, 3}},
			setIndex:      1,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}, {3, 3}},
		},
		{
			description:   "clears logs after non-matching term (snapshot)",
			snapshotIndex: 9,
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {2, 3}, {2, 4}, {2, 5}},
			setEntries:    []entry{{1, 1}, {1, 2}, {3, 3}},
			setIndex:      10,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}, {3, 3}},
		},
	}
	for _, test := range testCases {
		t.Run(test.description, func(t *testing.T) {
			rf := &Raft{
				log:      test.log,
				snapshot: snapshot{LastIndex: test.snapshotIndex},
			}

			rf.setLogEntries(test.setEntries, test.setIndex)

			if !reflect.DeepEqual(rf.log, test.want) {
				t.Errorf("log = %v, want %v", rf.log, test.want)
			}
		})
	}
}

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

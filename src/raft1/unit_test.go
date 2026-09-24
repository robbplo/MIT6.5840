package raft

import (
	"reflect"
	"testing"
)

func TestUnitSetLogEntries(t *testing.T) {
	t.Run("appends when own log is empty", func(t *testing.T) {
		rf := &Raft{
			log: []entry{{0, nil}},
		}

		rf.setLogEntries([]entry{{1, 1}, {1, 2}}, 1)

		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}}
		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})

	t.Run("overwrites existing logs with same term and preserves next", func(t *testing.T) {
		rf := &Raft{
			log: []entry{{0, nil}, {1, 0}, {1, 0}, {1, 0}},
		}

		rf.setLogEntries([]entry{{1, 1}, {1, 2}}, 1)

		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}, {1, 0}}
		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})

	t.Run("overwrites existing logs with same term and appends", func(t *testing.T) {
		rf := &Raft{
			log: []entry{{0, nil}, {1, 0}},
		}

		rf.setLogEntries([]entry{{1, 1}, {1, 2}}, 1)

		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}}
		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})

	t.Run("appends logs", func(t *testing.T) {
		rf := &Raft{
			log: []entry{{0, nil}, {1, 0}},
		}

		rf.setLogEntries([]entry{{1, 1}, {1, 2}}, 2)

		wantLog := []entry{{0, nil}, {1, 0}, {1, 1}, {1, 2}}
		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}

	})

	t.Run("overwrites logs from other term", func(t *testing.T) {
		rf := &Raft{
			log: []entry{{0, nil}, {1, 1}, {1, 2}, {2, 3}},
		}

		rf.setLogEntries([]entry{{3, 1}, {3, 2}, {3, 3}}, 1)

		wantLog := []entry{{0, nil}, {3, 1}, {3, 2}, {3, 3}}
		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}

	})
	t.Run("clears logs after non-matching term", func(t *testing.T) {
		rf := &Raft{
			log: []entry{{0, nil}, {1, 1}, {1, 2}, {2, 3}, {2, 4}, {2, 5}},
		}

		rf.setLogEntries([]entry{{1, 1}, {1, 2}, {3, 3}}, 1)

		wantLog := []entry{{0, nil}, {1, 1}, {1, 2}, {3, 3}}
		if !reflect.DeepEqual(rf.log, wantLog) {
			t.Errorf("log = %v, want %v", rf.log, wantLog)
		}
	})
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

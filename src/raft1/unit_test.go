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
		{
			description:   "handles out-of-bounds lower side",
			snapshotIndex: 9,
			log:           []entry{{0, nil}},
			setEntries:    []entry{{1, 1}, {1, 2}, {1, 3}},
			setIndex:      8,
			want:          []entry{{0, nil}, {1, 3}},
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
	testCases := []struct {
		description   string
		log           []entry
		snapshotIndex logIndex
		clearIndex    logIndex
		want          []entry
	}{
		{
			description:   "clears log after 'x' exclusive without snapshot",
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshotIndex: 0,
			clearIndex:    2,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}},
		},
		{
			description:   "clears log after 'x' exclusive with snapshot",
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshotIndex: 2,
			clearIndex:    4,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}},
		},
		{
			description:   "clears log after snapshot index",
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshotIndex: 2,
			clearIndex:    2,
			want:          []entry{{0, nil}},
		},
		{
			description: "checks for out-of-bounds",
			log:         []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			clearIndex:  3,
			want:        []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
		},
		{
			description:   "handles input below snapshot index",
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshotIndex: 50,
			clearIndex:    3,
			want:          []entry{{0, nil}},
		},
	}
	for _, test := range testCases {
		t.Run(test.description, func(t *testing.T) {
			rf := &Raft{
				log:      test.log,
				snapshot: snapshot{LastIndex: test.snapshotIndex},
			}
			rf.clearLogAfter(test.clearIndex)

			if !reflect.DeepEqual(rf.log, test.want) {
				t.Errorf("log = %v, want %v", rf.log, test.want)
			}
		})
	}
}

func TestUnitClearLogThrough(t *testing.T) {
	testCases := []struct {
		description   string
		log           []entry
		snapshotIndex logIndex
		clearIndex    logIndex
		want          []entry
	}{
		{
			description:   "clears log until 'x' inclusive without snapshot",
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshotIndex: 0,
			clearIndex:    2,
			want:          []entry{{0, nil}, {1, 3}},
		},
		{
			description:   "does not clear through the snapshot index",
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshotIndex: 2,
			clearIndex:    2,
			want:          []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
		},
		{
			description:   "clears log until 'x' inclusive with snapshot",
			log:           []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			snapshotIndex: 2,
			clearIndex:    4,
			want:          []entry{{0, nil}, {1, 3}},
		},
		{
			description: "checks for out-of-bounds",
			log:         []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
			clearIndex:  -5,
			want:        []entry{{0, nil}, {1, 1}, {1, 2}, {1, 3}},
		},
	}
	for _, test := range testCases {
		t.Run(test.description, func(t *testing.T) {
			rf := &Raft{
				log:      test.log,
				snapshot: snapshot{LastIndex: test.snapshotIndex},
			}
			rf.clearLogThrough(test.clearIndex)

			if !reflect.DeepEqual(rf.log, test.want) {
				t.Errorf("log = %v, want %v", rf.log, test.want)
			}
		})
	}
}

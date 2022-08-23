package raft

import "encoding/binary"

type LogEntry struct {
	Term  int64
	Index int64
	Data  []byte
}

type Log struct {
	logs    []*LogEntry
	storage Storage

	commitIndex int64
	lastApplied int64
}

func (l *Log) AppendEntry(entry []*LogEntry) {

	l.logs = append(l.logs, entry...)

	l.Apply()
}

func (l *Log) HasPrevLog(lastIndex, lastTerm int64) bool {

	for i := len(l.logs) - 1; i >= 0; i-- {
		if l.logs[i].Term == lastTerm && l.logs[i].Index == lastIndex {
			return true
		}
		if l.logs[i].Term < lastTerm {
			break
		}
	}

	return false
}

func (l *Log) RemoveConflictLog(entry *LogEntry) {

	conflictIdx := len(l.logs)
	for i := len(l.logs) - 1; i >= 0; i-- {
		if entry.Index == l.logs[i].Index && entry.Term != l.logs[i].Term {
			conflictIdx = i
		}
		if entry.Index > l.logs[i].Index {
			break
		}
	}

	if conflictIdx != len(l.logs) {
		l.logs = l.logs[:conflictIdx]
	}
}

func (l *Log) Apply() {
	if l.commitIndex > l.lastApplied {
		num := l.commitIndex - l.lastApplied
		for _, entry := range l.logs[len(l.logs)-int(num):] {
			key := make([]byte, 20)
			n := binary.PutUvarint(key, uint64(entry.Index))
			n += binary.PutUvarint(key, uint64(entry.Term))

			l.storage.Put(key[:n], entry.Data)
			l.lastApplied = entry.Index
		}
	}
}

func (l *Log) getLastLogTermAndIndex() (lastLogIndex, lastLogTerm int64) {
	if len(l.logs) > 0 {
		lastLog := l.logs[len(l.logs)-1]

		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	}
	return
}

func NewLog(storage Storage) *Log {

	return &Log{
		logs:    make([]*LogEntry, 0),
		storage: storage,
	}
}

package raft

import (
	"go.uber.org/zap"
)

type LogEntry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

type RaftLog struct {
	logs    []*LogEntry
	storage Storage

	commitIndex     uint64
	lastApplied     uint64
	lastAppliedTerm uint64

	logger *zap.SugaredLogger
}

func (l *RaftLog) GetEntries(index uint64) []*LogEntry {
	for i, entry := range l.logs {
		if entry.Index == index {
			return l.logs[i:]
		}
	}
	return l.logs[:0]

}

func (l *RaftLog) GetTerm(index uint64) uint64 {

	for _, entry := range l.logs {
		if entry.Index == index {
			return entry.Term
		}
	}

	if index == l.lastApplied {
		return l.lastAppliedTerm
	}

	return l.storage.GetTerm(index)
}

func (l *RaftLog) AppendEntry(entry []*LogEntry) {

	l.logs = append(l.logs, entry...)
}

func (l *RaftLog) HasPrevLog(lastIndex, lastTerm uint64) bool {
	if lastIndex == 0 {
		return true
	}

	return l.GetTerm(lastIndex) == lastTerm
}

func (l *RaftLog) RemoveConflictLog(entries []*LogEntry) {

	if len(entries) == 0 {
		return
	}
	entry := entries[0]

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
		l.logger.Debugf("删除冲突日志 %d", conflictIdx)
		l.logs = l.logs[:conflictIdx]
	}
}

func (l *RaftLog) Apply(lastCommit, lastLogIndex uint64) {
	if lastCommit > l.commitIndex {
		if lastLogIndex > lastCommit {
			l.commitIndex = lastCommit
		} else {
			l.commitIndex = lastLogIndex
		}
	}

	if l.commitIndex > l.lastApplied {
		n := 0
		for i, entry := range l.logs {
			if l.commitIndex <= entry.Index {
				n = i
			} else {
				break
			}
		}

		entries := l.logs[:n+1]
		l.storage.Append(entries)
		l.lastApplied = l.logs[n].Index
		l.lastAppliedTerm = l.logs[n].Term
		l.logs = l.logs[n+1:]
	}
}

func (l *RaftLog) GetLastLogIndexAndTerm() (lastLogIndex, lastLogTerm uint64) {
	if len(l.logs) > 0 {
		lastLog := l.logs[len(l.logs)-1]
		lastLogIndex = lastLog.Index
		lastLogTerm = lastLog.Term
	} else {
		lastLogIndex = l.lastApplied
		lastLogTerm = l.lastAppliedTerm
	}

	return
}

func NewRaftLog(storage Storage, logger *zap.SugaredLogger) *RaftLog {

	return &RaftLog{
		logs:    make([]*LogEntry, 0),
		storage: storage,
		logger:  logger,
	}
}

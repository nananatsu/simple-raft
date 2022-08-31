package raft

import (
	pb "kvdb/pkg/raftpb"

	"go.uber.org/zap"
)

const maxAppendEntriesSize = 1000

// type pb.LogEntry struct {
// 	Term  uint64
// 	Index uint64
// 	Data  []byte
// }

type RaftLog struct {
	logs    []*pb.LogEntry
	storage Storage

	commitIndex     uint64
	lastApplied     uint64
	lastAppliedTerm uint64

	logger *zap.SugaredLogger
}

func (l *RaftLog) GetEntries(index uint64) []*pb.LogEntry {
	var entries []*pb.LogEntry
	for i, entry := range l.logs {
		if entry.Index == index {
			entries = l.logs[i:]
			break
		}
	}

	if len(entries) > maxAppendEntriesSize {
		entries = entries[:maxAppendEntriesSize]
	}

	return entries

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

func (l *RaftLog) AppendEntry(entry []*pb.LogEntry) {

	size := len(entry)

	if size == 0 {
		return
	}

	if size > 1000 {
		l.logger.Debugf("添加日志: %d - %d", entry[0].Index, entry[size-1].Index)
	}

	l.logs = append(l.logs, entry...)
}

func (l *RaftLog) HasPrevLog(lastIndex, lastTerm uint64) bool {
	if lastIndex == 0 {
		return true
	}

	return l.GetTerm(lastIndex) == lastTerm
}

func (l *RaftLog) RemoveConflictLog(entries []*pb.LogEntry) []*pb.LogEntry {

	appendSize := len(entries)
	logSize := len(l.logs)
	if appendSize == 0 || logSize == 0 {
		return entries
	}

	conflictIdx := appendSize
	exsitIdx := -1
	prevIdx := -1
	for n, entry := range entries {
		for i := prevIdx + 1; i < logSize; i++ {
			le := l.logs[i]
			if entry.Index == le.Index {
				if entry.Term != le.Term {
					conflictIdx = i
					break
				} else {
					exsitIdx = n
					break
				}
			}
			prevIdx = i
		}
		if conflictIdx != appendSize {
			l.logger.Debugf("删除冲突日志 %d ~ %d", l.logs[conflictIdx].Index, l.logs[appendSize-1].Index)
			l.logs = l.logs[:conflictIdx]
			break
		}
	}

	if exsitIdx == -1 {
		return entries
	}
	l.logger.Debugf("修剪entry中已存在日志 %d ~ %d ", entries[0].Index, entries[exsitIdx].Index)
	return entries[exsitIdx+1:]
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
			if l.commitIndex >= entry.Index {
				n = i
			} else {
				break
			}
		}

		entries := l.logs[:n+1]

		if n > 100 {
			l.logger.Debugf("最后提交： %d ,已提交 %d ,提交日志: %d - %d", lastCommit, l.lastApplied, entries[0].Index, entries[len(entries)-1].Index)
		}

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
	// l.logger.Debugf("lastLogIndex: %d, lastLogTerm: %d, log size: %d", lastLogIndex, lastLogTerm, len(l.logs))

	return
}

func NewRaftLog(storage Storage, logger *zap.SugaredLogger) *RaftLog {

	return &RaftLog{
		logs:    make([]*pb.LogEntry, 0),
		storage: storage,
		logger:  logger,
	}
}

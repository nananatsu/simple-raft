package raft

type ReplicaProgress struct {
	MatchIndex uint64
	NextIndex  uint64
	pending    []uint64
	active     bool
}

func (rp *ReplicaProgress) AppendEntry(lastIndex uint64) {
	rp.pending = append(rp.pending, lastIndex)
	if rp.active {
		rp.NextIndex = lastIndex + 1
	}
}

func (rp *ReplicaProgress) AppendEntryResp(lastIndex uint64) {

	if rp.MatchIndex < lastIndex {
		rp.MatchIndex = lastIndex
	}

	idx := -1
	for i, v := range rp.pending {
		if v == lastIndex {
			idx = i
		}
	}

	if idx > -1 {
		if !rp.active {
			rp.active = true
			rp.NextIndex = lastIndex + 1
		}
		rp.pending = rp.pending[idx+1:]
	}
}

func (rp *ReplicaProgress) CanSend() bool {
	return rp.active || len(rp.pending) == 0
}

func (rp *ReplicaProgress) CheckPending() bool {
	if !rp.CanSend() {
		rp.pending = nil
		return false
	}
	return true
}

func (rp *ReplicaProgress) Reset(lastLogIndex uint64) {

	if lastLogIndex < rp.NextIndex {
		rp.NextIndex = lastLogIndex + 1
		rp.MatchIndex = lastLogIndex
	}

	if rp.active {
		rp.active = false
		rp.pending = nil
	}
}

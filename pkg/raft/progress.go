package raft

type ReplicaProgress struct {
	MatchIndex uint64
	NextIndex  uint64
	pending    []uint64
	prevResp   bool
}

func (rp *ReplicaProgress) AppendEntry(lastIndex uint64) {
	rp.pending = append(rp.pending, lastIndex)
	if rp.prevResp {
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
		if !rp.prevResp {
			rp.prevResp = true
			rp.NextIndex = lastIndex + 1
		}
		rp.pending = rp.pending[idx+1:]
	}
}

func (rp *ReplicaProgress) IsPause() bool {
	return !rp.prevResp && len(rp.pending) > 0
}

func (rp *ReplicaProgress) Reset(lastLogIndex uint64) {

	if lastLogIndex < rp.NextIndex {
		rp.NextIndex = lastLogIndex + 1
		rp.MatchIndex = lastLogIndex
	}

	if rp.prevResp {
		rp.prevResp = false
		rp.pending = nil
	}
}

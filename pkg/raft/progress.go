package raft

import pb "kvdb/pkg/raftpb"

type ReplicaProgress struct {
	MatchIndex         uint64
	NextIndex          uint64
	pending            []uint64
	prevResp           bool
	maybeLostIndex     uint64
	installingSnapshot bool
	snapc              chan *pb.Snapshot
	prevSnap           *pb.Snapshot
	maybePrevSnapLost  *pb.Snapshot
}

func (rp *ReplicaProgress) MaybeLogLost(index uint64) bool {
	if index == rp.maybeLostIndex {
		rp.maybeLostIndex = 0
		return true
	}
	rp.maybeLostIndex = index
	return false
}

func (rp *ReplicaProgress) MaybeSnapLost(snap *pb.Snapshot) bool {
	if rp.maybePrevSnapLost != nil && rp.maybePrevSnapLost == snap {
		rp.maybePrevSnapLost = nil
		return true
	}
	rp.maybePrevSnapLost = snap
	return false
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

func (rp *ReplicaProgress) InstallSnapshot(snapc chan *pb.Snapshot) {
	rp.installingSnapshot = true
	rp.snapc = snapc
}

func (rp *ReplicaProgress) GetSnapshot(prevSuccess bool) *pb.Snapshot {

	if !prevSuccess {
		return rp.prevSnap
	}

	if rp.snapc == nil {
		return nil
	}
	snap := <-rp.snapc
	if snap == nil {
		rp.snapc = nil
		rp.installingSnapshot = false
	}
	rp.prevSnap = snap

	return snap
}

func (rp *ReplicaProgress) IsPause() bool {
	return rp.installingSnapshot || (!rp.prevResp && len(rp.pending) > 0)
}

func (rp *ReplicaProgress) ResetLogIndex(lastLogIndex uint64, leaderLastLogIndex uint64) {

	if lastLogIndex < leaderLastLogIndex {
		rp.NextIndex = lastLogIndex + 1
		rp.MatchIndex = lastLogIndex
	} else {
		rp.NextIndex = leaderLastLogIndex + 1
		rp.MatchIndex = leaderLastLogIndex
	}

	if rp.prevResp {
		rp.prevResp = false
		rp.pending = nil
	}
}
func (rp *ReplicaProgress) Reset() {

	rp.pending = rp.pending[:0]
	rp.prevResp = false
	rp.maybeLostIndex = 0
	rp.installingSnapshot = false
	if rp.snapc != nil {
		close(rp.snapc)
		rp.snapc = nil
	}
	rp.prevSnap = nil
	rp.maybePrevSnapLost = nil
}

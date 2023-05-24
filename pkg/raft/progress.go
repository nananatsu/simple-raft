package raft

import pb "kvdb/pkg/raftpb"

// 节点同步状态
type ReplicaProgress struct {
	MatchIndex         uint64            // 已接收日志
	NextIndex          uint64            // 下次发送日志
	pending            []uint64          // 未发送完成日志
	prevResp           bool              // 上次日志发送结果
	maybeLostIndex     uint64            // 可能丢失的日志,记上次发送未完以重发
	installingSnapshot bool              // 是否发送快照中
	snapc              chan *pb.Snapshot // 快照读取通道
	prevSnap           *pb.Snapshot      // 上次发送快照
	maybePrevSnapLost  *pb.Snapshot      // 可能丢失快照,标记上次发送未完成以重发
}

// 标记日志可能发送失败
func (rp *ReplicaProgress) MaybeLogLost(index uint64) bool {
	if index == rp.maybeLostIndex {
		rp.maybeLostIndex = 0
		return true
	}
	rp.maybeLostIndex = index
	return false
}

// 标记快照可能发送失败
func (rp *ReplicaProgress) MaybeSnapLost(snap *pb.Snapshot) bool {
	if rp.maybePrevSnapLost != nil && rp.maybePrevSnapLost == snap {
		rp.maybePrevSnapLost = nil
		return true
	}
	rp.maybePrevSnapLost = snap
	return false
}

// 记录日志发送中,如上次发送成功直接更新下次发送日志索引
func (rp *ReplicaProgress) AppendEntry(lastIndex uint64) {
	rp.pending = append(rp.pending, lastIndex)
	if rp.prevResp {
		rp.NextIndex = lastIndex + 1
	}
}

// 移除日志发送中,更新已接收日志
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

	// 标记前次日志发送成功，更新下次发送
	if !rp.prevResp {
		rp.prevResp = true
		rp.NextIndex = lastIndex + 1
	}

	if idx > -1 {
		// 清除之前发送
		rp.pending = rp.pending[idx+1:]
	}
}

// 标记正在发送快照
func (rp *ReplicaProgress) InstallSnapshot(snapc chan *pb.Snapshot) {
	rp.installingSnapshot = true
	rp.snapc = snapc
}

// 从快照对去通道获取快照
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

// 检查是否暂停发送,第一次发送未完成或安装正在发送快照时暂停日志发送
func (rp *ReplicaProgress) IsPause() bool {
	return rp.installingSnapshot || (!rp.prevResp && len(rp.pending) > 0)
}

// 更新日志发送进度
func (rp *ReplicaProgress) ResetLogIndex(lastLogIndex uint64, leaderLastLogIndex uint64) {

	// 节点最后日志小于leader最新日志按节点更新进度，否则按leader更新进度
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

// 重置状态
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

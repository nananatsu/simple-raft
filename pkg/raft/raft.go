package raft

import (
	"encoding/json"
	"math/rand"
	"strconv"

	pb "kvdb/pkg/raftpb"

	"go.uber.org/zap"
)

const MAX_LOG_ENTRY_SEND = 1000

// raft节点类型
type RaftState int

// raft 节点类型
const (
	CANDIDATE_STATE RaftState = iota
	FOLLOWER_STATE
	LEADER_STATE
)

type Raft struct {
	id                    uint64
	state                 RaftState             // 节点类型
	leader                uint64                // leader id
	currentTerm           uint64                // 当前任期
	voteFor               uint64                // 投票对象
	raftlog               *RaftLog              // 日志
	cluster               *Cluster              // 集群节点
	electionTimeout       int                   // 选取周期
	heartbeatTimeout      int                   // 心跳周期
	randomElectionTimeout int                   // 随机选取周期
	electtionTick         int                   // 选取时钟
	hearbeatTick          int                   // 心跳时钟
	Tick                  func()                // 时钟函数,Leader为心跳时钟，其他为选取时钟
	hanbleMessage         func(*pb.RaftMessage) // 消息处理函数,按节点状态对应不通处理
	Msg                   []*pb.RaftMessage     // 待发送消息
	logger                *zap.SugaredLogger
}

// 切换点点为Candidate
func (r *Raft) SwitchCandidate() {
	r.state = CANDIDATE_STATE
	r.leader = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.hanbleMessage = r.HandleCandidateMessage

	lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()

	r.cluster.Foreach(func(_ uint64, p *ReplicaProgress) {
		p.NextIndex = lastIndex + 1
		p.MatchIndex = lastIndex
	})

	r.BroadcastRequestVote()
	r.electtionTick = 0
	r.logger.Debugf("成为候选者, 任期 %d , 选取周期 %d s", r.currentTerm, r.randomElectionTimeout)
}

// 切换节点为Follower
func (r *Raft) SwitchFollower(leaderId, term uint64) {

	r.state = FOLLOWER_STATE
	r.leader = leaderId
	r.currentTerm = term
	r.voteFor = 0
	// r.cluster.ResetVoteResult()
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.hanbleMessage = r.HandleFollowerMessage
	r.electtionTick = 0
	r.cluster.Reset()

	r.logger.Debugf("成为追随者, 领导者 %s, 任期 %d , 选取周期 %d s", strconv.FormatUint(leaderId, 16), term, r.randomElectionTimeout)
}

// 切换节点为Leader
func (r *Raft) SwitchLeader() {
	r.logger.Debugf("成为领导者, 任期: %d", r.currentTerm)

	r.state = LEADER_STATE
	r.leader = r.id
	r.voteFor = 0
	// r.cluster.ResetVoteResult()
	r.Tick = r.TickHeartbeat
	r.hanbleMessage = r.HandleLeaderMessage
	r.BroadcastHeartbeat()
	r.electtionTick = 0
	r.hearbeatTick = 0
	r.cluster.Reset()
	r.cluster.pendingChangeIndex = r.raftlog.lastAppliedIndex
}

// 心跳时钟跳动
func (r *Raft) TickHeartbeat() {
	r.hearbeatTick++

	// r.logger.Debugf("心跳时钟推进 %d", r.hearbeatTick)

	lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()

	if r.hearbeatTick >= r.heartbeatTimeout {
		r.hearbeatTick = 0
		r.BroadcastHeartbeat()
		r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
			if id == r.id {
				return
			}

			pendding := len(p.pending)
			// 重发消息
			if !p.prevResp && pendding > 0 && p.MaybeLogLost(p.pending[0]) || (pendding == 0 && p.NextIndex <= lastIndex) {
				p.pending = nil
				r.SendAppendEntries(id)
			}

			// 重发快照
			if p.installingSnapshot && p.prevSnap != nil && p.MaybeSnapLost(p.prevSnap) {
				r.logger.Debugf("重发 %d_%s@%d_%d 偏移 %d", p.prevSnap.Level, strconv.FormatUint(p.prevSnap.LastIncludeIndex, 16), p.prevSnap.LastIncludeTerm, p.prevSnap.Segment, p.prevSnap.Offset)
				r.SendSnapshot(id, false)
			}

		})
	}

}

// 选取时钟跳动
func (r *Raft) TickElection() {
	r.electtionTick++

	if r.electtionTick >= r.randomElectionTimeout {
		r.electtionTick = 0
		if r.state == CANDIDATE_STATE {
			r.BroadcastRequestVote()
		}
		if r.state == FOLLOWER_STATE {
			r.SwitchCandidate()
		}
	}

}

// 处理消息
func (r *Raft) HandleMessage(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}

	r.hanbleMessage(msg)
}

// 候选人处理消息
func (r *Raft) HandleCandidateMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE:
		b := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.electtionTick = 0
		}
	case pb.MessageType_VOTE_RESP:
		r.ReciveVoteResp(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.Success)
	case pb.MessageType_HEARTBEAT:
		if r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit) {
			r.SwitchFollower(msg.From, msg.Term)
		}
	case pb.MessageType_APPEND_ENTRY:
		b := r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		if b {
			r.SwitchFollower(msg.From, msg.Term)
		}
	default:
		m, _ := json.Marshal(msg)
		r.logger.Debugf("收到异常消息 %s", string(m))
	}

	// if msg.Term > r.currentTerm {
	// 	r.SwitchFollower(msg.From, msg.Term)
	// }

}

// 追随者处理消息
func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE:
		b := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.electtionTick = 0
		}
	case pb.MessageType_HEARTBEAT:
		if r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit) {
			r.electtionTick = 0
		}
	case pb.MessageType_APPEND_ENTRY:
		b := r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		if b {
			r.electtionTick = 0
		}
	case pb.MessageType_PROPOSE:
		r.SendMessage(pb.MessageType_PROPOSE, r.leader, 0, 0, 0, msg.Entry, false)
	case pb.MessageType_PROPOSE_RESP:
		break
	case pb.MessageType_INSTALL_SNAPSHOT:
		r.ReciveInstallSnapshot(msg.From, msg.Term, msg.Snapshot)
	default:
		m, _ := json.Marshal(msg)
		r.logger.Debugf("收到异常消息 %s", string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}
}

// 领导者处理消息
func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_PROPOSE:
		r.AppendEntry(msg.Entry)
	// case pb.MessageType_VOTE:
	// r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
	case pb.MessageType_VOTE_RESP:
		break
	case pb.MessageType_HEARTBEAT_RESP:
		r.ReciveHeartbeatResp(msg.From, msg.Term, msg.LastLogIndex)
	case pb.MessageType_APPEND_ENTRY_RESP:
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	case pb.MessageType_INSTALL_SNAPSHOT_RESP:
		r.ReciveInstallSnapshotResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	default:
		m, _ := json.Marshal(msg)
		r.logger.Infof("收到异常消息 %s", string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}
}

// Leader 添加日志
func (r *Raft) AppendEntry(entries []*pb.LogEntry) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	for i, entry := range entries {
		if entry.Type == pb.EntryType_MEMBER_CHNAGE {
			r.cluster.pendingChangeIndex = entry.Index
			r.cluster.inJoint = true
		}
		entry.Index = lastLogIndex + 1 + uint64(i)
		entry.Term = r.currentTerm
	}
	r.raftlog.AppendEntry(entries)
	r.cluster.UpdateLogIndex(r.id, entries[len(entries)-1].Index)

	r.BroadcastAppendEntries()
}

//  变更集群成员
func (r *Raft) ApplyChange(change []*pb.MemberChange) error {
	return r.cluster.ApplyChange(change)
}

// 广播日志
func (r *Raft) BroadcastAppendEntries() {
	r.cluster.Foreach(func(id uint64, _ *ReplicaProgress) {
		if id == r.id {
			return
		}
		r.SendAppendEntries(id)
	})
}

// 广播心跳
func (r *Raft) BroadcastHeartbeat() {
	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex := p.NextIndex - 1
		lastLogTerm := r.raftlog.GetTerm(lastLogIndex)

		// r.logger.Debugf("发送心跳到 %s", strconv.FormatUint(id, 16))

		r.SendMessage(pb.MessageType_HEARTBEAT, id, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, nil, false)
	})
}

// 广播选取
func (r *Raft) BroadcastRequestVote() {
	r.currentTerm++
	r.voteFor = r.id
	r.cluster.ResetVoteResult()
	r.cluster.Vote(r.id, true)

	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		r.SendMessage(pb.MessageType_VOTE, id, lastLogIndex, lastLogTerm, 0, nil, false)
	})

}

// 发送日志到指定节点
func (r *Raft) SendAppendEntries(to uint64) {
	p := r.cluster.progress[to]
	if p == nil || p.IsPause() {
		// r.logger.Debugf("节点 %s 停止发送消息, 上次发送状态: %t ,未确认消息 %d ", strconv.FormatUint(to, 16), r.cluster.progress[to].prevResp, len(r.cluster.progress[to].pending))
		return
	}

	nextIndex := r.cluster.GetNextIndex(to)
	lastLogIndex := nextIndex - 1
	lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
	maxSize := MAX_LOG_ENTRY_SEND

	if !p.prevResp {
		maxSize = 1
	}
	// var entries []*pb.LogEntry
	entries := r.raftlog.GetEntries(nextIndex, maxSize)
	size := len(entries)
	if size == 0 {
		if nextIndex <= r.raftlog.lastAppliedIndex {
			if !p.prevResp { // 前次发送未成功时不发送快照，将下次发送日志重置到leader最新，待发送成功后再同步快照
				r.logger.Debugf("%s 前次发送未成功,暂缓同步快照", strconv.FormatUint(to, 16))
				r.SendMessage(pb.MessageType_APPEND_ENTRY, to, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, nil, false)
				return
			} else {
				snapc, err := r.raftlog.GetSnapshot(nextIndex)
				if err != nil {
					r.logger.Errorf("获取快照失败: %v", err)
					return
				}

				r.cluster.InstallSnapshot(to, snapc)
				r.SendSnapshot(to, true)
				return
			}
		} else {
			// r.logger.Debugf("节点 %s 下次发送: %d, 当前已同步到最新日志", strconv.FormatUint(to, 16), nextIndex)
			r.SendMessage(pb.MessageType_APPEND_ENTRY, to, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, entries, false)
			return
		}
	}
	// r.logger.Debugf("发送日志到 %s, 范围 %d ~ %d ", strconv.FormatUint(to, 16), entries[0].Index, entries[size-1].Index)
	r.cluster.AppendEntry(to, entries[size-1].Index)
	r.SendMessage(pb.MessageType_APPEND_ENTRY, to, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, entries, false)
}

// 发送快照到指定节点
func (r *Raft) SendSnapshot(to uint64, prevSuccess bool) {
	snap := r.cluster.GetSnapshot(to, prevSuccess)
	if snap == nil {
		r.SendAppendEntries(to)
		return
	}

	// r.logger.Debugf("发送%s  %d_%s@%d_%d 偏移 %d", strconv.FormatUint(to, 16), snap.Level, strconv.FormatUint(snap.LastIncludeIndex, 16), snap.LastIncludeTerm, snap.Segment, snap.Offset)

	msg := &pb.RaftMessage{
		MsgType:  pb.MessageType_INSTALL_SNAPSHOT,
		Term:     r.currentTerm,
		From:     r.id,
		To:       to,
		Snapshot: snap,
	}

	r.Msg = append(r.Msg, msg)
}

// 发送消息
func (r *Raft) SendMessage(msgType pb.MessageType, to, lastLogIndex, lastLogTerm, LastCommit uint64, entry []*pb.LogEntry, success bool) {

	// r.logger.Debugf("发送: %s 到: %s", msgType.string(), strconv.FormatUint(to, 16))
	r.Msg = append(r.Msg, &pb.RaftMessage{
		MsgType:      msgType,
		Term:         r.currentTerm,
		From:         r.id,
		To:           to,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		LastCommit:   r.raftlog.commitIndex,
		Entry:        entry,
		Success:      success,
	})
}

// 处理选取响应
func (r *Raft) ReciveVoteResp(from, term, lastLogTerm, lastLogIndex uint64, success bool) {

	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.cluster.Vote(from, success)
	r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)

	voteRes := r.cluster.CheckVoteResult()
	if voteRes == VoteWon {
		r.logger.Debugf("节点 %s 发起投票, 赢得选取", strconv.FormatUint(r.id, 16))
		for k, v := range r.cluster.voteResp {
			if !v {
				r.cluster.ResetLogIndex(k, lastLogIndex, leaderLastLogIndex)
			}
		}
		r.SwitchLeader()
	} else if voteRes == VoteLost {
		r.logger.Debugf("节点 %s 发起投票, 输掉选取", strconv.FormatUint(r.id, 16))
		r.voteFor = 0
		r.cluster.ResetVoteResult()
	}
}

// 处理日志添加响应
func (r *Raft) ReciveAppendEntriesResult(from, term, lastLogIndex uint64, success bool) {

	leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()

	if success {
		r.cluster.AppendEntryResp(from, lastLogIndex)
		if lastLogIndex > r.raftlog.commitIndex {
			// 取已同步索引更新到lastcommit
			if r.cluster.CheckCommit(lastLogIndex) {
				prevApplied := r.raftlog.lastAppliedIndex
				r.raftlog.Apply(lastLogIndex, lastLogIndex)
				r.BroadcastAppendEntries()

				// 检查联合共识是否完成
				if r.cluster.inJoint && prevApplied < r.cluster.pendingChangeIndex && lastLogIndex >= r.cluster.pendingChangeIndex {
					r.AppendEntry([]*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE}})
					lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
					r.cluster.pendingChangeIndex = lastIndex
				}
			}
		}
		if r.cluster.GetNextIndex(from) <= leaderLastLogIndex {
			r.SendAppendEntries(from)
		}
	} else {
		r.logger.Infof("节点 %s 追加日志失败, Leader记录节点最新日志: %d ,节点最新日志: %d ", strconv.FormatUint(from, 16), r.cluster.GetNextIndex(from)-1, lastLogIndex)

		r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
		r.SendAppendEntries(from)
	}
}

// 处理快照发送响应
func (r *Raft) ReciveInstallSnapshotResult(from, term, lastLogIndex uint64, installed bool) {

	if term < r.currentTerm {
		return
	} else {
		if installed {
			leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
			r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
			r.logger.Debugf("%s 快照更新 ,当前最后日志 %d ", strconv.FormatUint(from, 16), lastLogIndex)
		}
		r.SendSnapshot(from, true)
	}
}

// 处理快照
func (r *Raft) ReciveInstallSnapshot(from, term uint64, snap *pb.Snapshot) {

	var success bool
	if term < r.currentTerm {
		return
	} else if snap.LastIncludeIndex > r.raftlog.lastAppliedIndex {
		// r.logger.Debugf("收到%s  %d_%s@%d_%d 偏移 %d", strconv.FormatUint(from, 16), snap.Level, strconv.FormatUint(snap.LastIncludeIndex, 16), snap.LastIncludeTerm, snap.Segment, snap.Offset)
		success, _ = r.raftlog.InstallSnapshot(snap)
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	r.SendMessage(pb.MessageType_INSTALL_SNAPSHOT_RESP, from, lastLogIndex, lastLogTerm, 0, nil, success)

}

// 处理心跳响应
func (r *Raft) ReciveHeartbeatResp(mFrom, mTerm, mLastLogIndex uint64) {
	p := r.cluster.progress[mFrom]
	if mTerm >= r.currentTerm && p != nil && len(p.pending) == 0 && mLastLogIndex < p.NextIndex {
		lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		p.ResetLogIndex(mLastLogIndex, lastLogIndex)
	}
}

// 处理心跳
func (r *Raft) ReciveHeartbeat(mFrom, mTerm, mLastLogIndex, mLastCommit uint64) bool {

	// r.logger.Debugf("收到心跳到 %s", strconv.FormatUint(mFrom, 16))
	lastLogIndex, lastAppliedTerm := r.raftlog.GetLastLogIndexAndTerm()
	accept := mTerm >= r.currentTerm
	if accept {
		r.raftlog.Apply(mLastCommit, lastLogIndex)
	}
	r.SendMessage(pb.MessageType_HEARTBEAT_RESP, mFrom, lastLogIndex, lastAppliedTerm, 0, nil, accept)
	return accept
}

// 处理日志
func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*pb.LogEntry) (success bool) {
	if mTerm < r.currentTerm {
		r.logger.Infof("消息任期%d 小于当前任期 %d", mTerm, r.currentTerm)
		success = false
	} else if !r.raftlog.HasPrevLog(mLastLogIndex, mLastLogTerm) { // 检查节点是否拥有leader最后提交日志

		r.logger.Infof("节点未含有上次追加日志: Index: %d, Term: %d ,新增日志: %d ~ %d", mLastLogIndex, mLastLogTerm, mEntries[0].Index, mEntries[len(mEntries)-1].Index)
		success = false
	} else {
		r.raftlog.AppendEntry(r.raftlog.RemoveConflictLog(mEntries))
		success = true
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)

	r.SendMessage(pb.MessageType_APPEND_ENTRY_RESP, mLeader, lastLogIndex, lastLogTerm, 0, nil, success)
	return
}

// 处理选取
func (r *Raft) ReciveRequestVote(mTerm, mCandidateId, mLastLogTerm, mLastLogIndex uint64) (success bool) {

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()

	if mTerm < r.currentTerm {
		success = false
	} else if r.voteFor == 0 || r.voteFor == mCandidateId {
		if mLastLogTerm >= lastLogTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			success = true
			// r.currentTerm = mTerm
		}
	}

	r.logger.Debugf("候选人: %s, 投票: %t ,任期: %d ,最新日志: %d, 最新日志任期: %d, 节点最新日志: %d ,节点最新日志任期： %d ", strconv.FormatUint(mCandidateId, 16), success, mTerm, mLastLogIndex, mLastLogTerm, lastLogIndex, lastLogTerm)
	r.SendMessage(pb.MessageType_VOTE_RESP, mCandidateId, lastLogIndex, lastLogTerm, 0, nil, success)
	return
}

func NewRaft(id uint64, storage Storage, peers map[uint64]string, logger *zap.SugaredLogger) *Raft {

	raftlog := NewRaftLog(storage, logger)
	raft := &Raft{
		id:               id,
		currentTerm:      raftlog.lastAppliedTerm,
		raftlog:          raftlog,
		cluster:          NewCluster(peers, raftlog.commitIndex, logger),
		electionTimeout:  10,
		heartbeatTimeout: 5,
		logger:           logger,
	}

	logger.Infof("实例: %s ,任期: %d ", strconv.FormatUint(raft.id, 16), raft.currentTerm)
	raft.SwitchFollower(0, raft.currentTerm)

	return raft
}

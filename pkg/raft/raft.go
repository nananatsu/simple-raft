package raft

import (
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
	hanbleMessage         func(*pb.RaftMessage) // 消息处理函数,按节点状态对应不同处理
	Msg                   []*pb.RaftMessage     // 待发送消息
	ReadIndex             []*ReadIndexResp      // 检查Leader完成的readindex
	logger                *zap.SugaredLogger
}

// 切换点点为Candidate
func (r *Raft) SwitchCandidate() {
	r.state = CANDIDATE_STATE
	r.leader = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.hanbleMessage = r.HandleCandidateMessage

	// 以节点最新日志，重置同步进度状态，leader按进度发送日志
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
		r.BroadcastHeartbeat(nil)
		r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
			if id == r.id {
				return
			}

			pendding := len(p.pending)
			// 重发消息，重发条件：
			// 上次消息发送未响应且当前有发送未完成,且上次心跳该消息就已处于等待响应状态
			// 当前无等待响应消息，且节点下次发送日志小于leader最新日志
			if !p.prevResp && pendding > 0 && p.MaybeLogLost(p.pending[0]) || (pendding == 0 && p.NextIndex <= lastIndex) {
				p.pending = nil
				r.SendAppendEntries(id)
			}

			// 重发快照,条件：上次快照在两次心跳内未发送完成
			if p.installingSnapshot && p.prevSnap != nil && p.MaybeSnapLost(p.prevSnap) {
				r.logger.Debugf("重发 %d_%s@%d_%d 偏移 %d", p.prevSnap.Level, strconv.FormatUint(p.prevSnap.LastIncludeIndex, 16), p.prevSnap.LastIncludeTerm, p.prevSnap.Segment, p.prevSnap.Offset)
				r.sendSnapshot(id, false)
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

	// 消息任期小于节点任期,拒绝消息: 1、网络延迟，节点任期是集群任期; 2、网络断开,节点增加了任期，集群任期是消息任期
	if msg.Term < r.currentTerm {
		r.logger.Debugf("收到来自 %s 过期 (%d) %s 消息 ", strconv.FormatUint(msg.From, 16), msg.Term, msg.MsgType)
		return
	} else if msg.Term > r.currentTerm {
		// 消息非请求投票，集群发生选取，新任期产生
		if msg.MsgType != pb.MessageType_VOTE {
			// 日志追加、心跳、快照为leader发出，，节点成为该leader追随者
			if msg.MsgType == pb.MessageType_APPEND_ENTRY || msg.MsgType == pb.MessageType_HEARTBEAT || msg.MsgType == pb.MessageType_INSTALL_SNAPSHOT {
				r.SwitchFollower(msg.From, msg.Term)
			} else { // 变更节点为追随者，等待leader消息
				r.SwitchFollower(msg.From, 0)
			}
		}
	}

	r.hanbleMessage(msg)
}

// 候选人处理消息
func (r *Raft) HandleCandidateMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE:
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant { // 投票后重置选取时间
			r.electtionTick = 0
		}
	case pb.MessageType_VOTE_RESP:
		r.ReciveVoteResp(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.Success)
	case pb.MessageType_HEARTBEAT:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)
	case pb.MessageType_APPEND_ENTRY:
		r.SwitchFollower(msg.From, msg.Term)
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// 追随者处理消息
func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_VOTE:
		grant := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if grant {
			r.electtionTick = 0
		}
	case pb.MessageType_READINDEX: // 询问Leader最新提交
		lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		r.cluster.AddReadIndex(msg.From, lastLogIndex, msg.Context)
		msg.To = r.leader
		msg.From = r.id
		r.send(msg)
	case pb.MessageType_READINDEX_RESP:
		r.ReadIndex = append(r.ReadIndex, &ReadIndexResp{
			Req:   msg.Context,
			Index: msg.LastLogIndex,
			Send:  msg.To,
		})
	case pb.MessageType_HEARTBEAT:
		r.electtionTick = 0
		r.ReciveHeartbeat(msg.From, msg.Term, msg.LastLogIndex, msg.LastCommit, msg.Context)
	case pb.MessageType_APPEND_ENTRY:
		r.electtionTick = 0
		r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
	case pb.MessageType_PROPOSE:
		msg.To = r.leader
		msg.Term = r.currentTerm
		msg.From = r.id
		r.send(msg)
	case pb.MessageType_INSTALL_SNAPSHOT:
		r.ReciveInstallSnapshot(msg.From, msg.Term, msg.Snapshot)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
	}
}

// 领导者处理消息
func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {
	switch msg.MsgType {
	case pb.MessageType_PROPOSE:
		r.AppendEntry(msg.Entry)
	case pb.MessageType_READINDEX: // readindex向集群发送心跳检查是否为Leader
		r.BroadcastHeartbeat(msg.Context)
		r.hearbeatTick = 0

		lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		r.cluster.AddReadIndex(msg.From, lastLogIndex, msg.Context)
	case pb.MessageType_VOTE:
		r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
	case pb.MessageType_VOTE_RESP:
		break
	case pb.MessageType_HEARTBEAT_RESP:
		r.ReciveHeartbeatResp(msg.From, msg.Term, msg.LastLogIndex, msg.Context)
	case pb.MessageType_APPEND_ENTRY_RESP:
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	case pb.MessageType_INSTALL_SNAPSHOT_RESP:
		r.ReciveInstallSnapshotResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	default:
		r.logger.Debugf("收到 %s 异常消息 %s 任期 %d", strconv.FormatUint(msg.From, 16), msg.MsgType, msg.Term)
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

// 变更集群成员
func (r *Raft) ApplyChange(change []*pb.MemberChange) error {
	err := r.cluster.ApplyChange(change)
	if err == nil && r.state == LEADER_STATE {
		r.BroadcastAppendEntries()
	}
	return err
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
func (r *Raft) BroadcastHeartbeat(context []byte) {
	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex := p.NextIndex - 1
		lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
		r.send(&pb.RaftMessage{
			MsgType:      pb.MessageType_HEARTBEAT,
			Term:         r.currentTerm,
			From:         r.id,
			To:           id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
			LastCommit:   r.raftlog.commitIndex,
			Context:      context,
		})
		// r.logger.Debugf("发送心跳到 %s", strconv.FormatUint(id, 16))
	})
}

// 广播选取
func (r *Raft) BroadcastRequestVote() {
	r.currentTerm++
	r.voteFor = r.id
	r.cluster.ResetVoteResult()
	r.cluster.Vote(r.id, true)

	r.logger.Infof("%s 发起投票", strconv.FormatUint(r.id, 16))

	r.cluster.Foreach(func(id uint64, p *ReplicaProgress) {
		if id == r.id {
			return
		}
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		r.send(&pb.RaftMessage{
			MsgType:      pb.MessageType_VOTE,
			Term:         r.currentTerm,
			From:         r.id,
			To:           id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		})
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
		if nextIndex <= r.raftlog.lastAppliedIndex && p.prevResp {
			snapc, err := r.raftlog.GetSnapshot(nextIndex)
			if err != nil {
				r.logger.Errorf("获取快照失败: %v", err)
				return
			}

			r.cluster.InstallSnapshot(to, snapc)
			r.sendSnapshot(to, true)
			return
		}
	} else {
		r.cluster.AppendEntry(to, entries[size-1].Index)
	}

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY,
		Term:         r.currentTerm,
		From:         r.id,
		To:           to,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		LastCommit:   r.raftlog.commitIndex,
		Entry:        entries,
	})
}

// 发送快照到指定节点
func (r *Raft) sendSnapshot(to uint64, prevSuccess bool) {
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

func (r *Raft) send(msg *pb.RaftMessage) {
	r.Msg = append(r.Msg, msg)
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
		r.BroadcastAppendEntries()
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
		} else if len(r.raftlog.waitQueue) > 0 {
			r.raftlog.NotifyReadIndex()
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
	if installed {
		leaderLastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		r.cluster.ResetLogIndex(from, lastLogIndex, leaderLastLogIndex)
		r.logger.Debugf("%s 快照更新 ,当前最后日志 %d ", strconv.FormatUint(from, 16), lastLogIndex)
	}
	r.sendSnapshot(from, true)

}

// 处理快照
func (r *Raft) ReciveInstallSnapshot(from, term uint64, snap *pb.Snapshot) {
	var installed bool
	if snap.LastIncludeIndex > r.raftlog.lastAppliedIndex {
		// r.logger.Debugf("收到%s  %d_%s@%d_%d 偏移 %d", strconv.FormatUint(from, 16), snap.Level, strconv.FormatUint(snap.LastIncludeIndex, 16), snap.LastIncludeTerm, snap.Segment, snap.Offset)
		installed, _ = r.raftlog.InstallSnapshot(snap)
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_INSTALL_SNAPSHOT_RESP,
		Term:         r.currentTerm,
		From:         r.id,
		To:           from,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      installed,
	})
}

// 处理心跳响应
func (r *Raft) ReciveHeartbeatResp(mFrom, mTerm, mLastLogIndex uint64, context []byte) {

	if len(context) > 0 {
		resp := r.cluster.HeartbeatCheck(context, mFrom)
		if resp != nil {
			// 响应follower readindex 请求
			if resp.Send != 0 && resp.Send != r.id {
				r.send(&pb.RaftMessage{
					MsgType:      pb.MessageType_READINDEX_RESP,
					Term:         r.currentTerm,
					From:         r.id,
					To:           resp.Send,
					LastLogIndex: resp.Index,
					Context:      context,
				})
			} else {
				r.ReadIndex = append(r.ReadIndex, resp)
			}
		}
	}

	// p := r.cluster.progress[mFrom]
	// if p != nil && len(p.pending) == 0 && mLastLogIndex < p.NextIndex {
	// 	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	// 	p.ResetLogIndex(mLastLogIndex, lastLogIndex)
	// }
}

// 处理心跳
func (r *Raft) ReciveHeartbeat(mFrom, mTerm, mLastLogIndex, mLastCommit uint64, context []byte) {
	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)

	r.send(&pb.RaftMessage{
		MsgType: pb.MessageType_HEARTBEAT_RESP,
		Term:    r.currentTerm,
		From:    r.id,
		To:      mFrom,
		Context: context,
	})
}

// 处理日志
func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*pb.LogEntry) {

	var accept bool
	if !r.raftlog.HasPrevLog(mLastLogIndex, mLastLogTerm) { // 检查节点日志是否与leader一致
		r.logger.Infof("节点未含有上次追加日志: Index: %d, Term: %d ", mLastLogIndex, mLastLogTerm)
		accept = false
	} else {
		r.raftlog.AppendEntry(mEntries)
		accept = true
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)
	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_APPEND_ENTRY_RESP,
		Term:         r.currentTerm,
		From:         r.id,
		To:           mLeader,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      accept,
	})
}

// 处理选取
func (r *Raft) ReciveRequestVote(mTerm, mCandidateId, mLastLogTerm, mLastLogIndex uint64) (success bool) {

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	if r.voteFor == 0 || r.voteFor == mCandidateId {
		if mTerm > r.currentTerm && mLastLogTerm >= lastLogTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			success = true
		}
	}

	r.logger.Debugf("候选人: %s, 投票: %t ", strconv.FormatUint(mCandidateId, 16), success)

	r.send(&pb.RaftMessage{
		MsgType:      pb.MessageType_VOTE_RESP,
		Term:         mTerm,
		From:         r.id,
		To:           mCandidateId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
		Success:      success,
	})
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

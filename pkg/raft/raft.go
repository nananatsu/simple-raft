package raft

import (
	"encoding/json"
	"math/rand"

	pb "kvdb/pkg/raftpb"

	"go.uber.org/zap"
)

// raft节点类型

type RaftState int

const (
	CANDIDATE_STATE RaftState = iota
	FOLLOWER_STATE
	LEADER_STATE
)

type Raft struct {
	// mu               sync.RWMutex
	id                    uint64
	state                 RaftState
	leader                uint64
	currentTerm           uint64
	voteFor               uint64
	voteResult            map[uint64]bool
	raftlog               *RaftLog
	replica               map[uint64]*ReplicaProgress
	electionTimeout       int
	heartbeatTimeout      int
	randomElectionTimeout int
	electtionTick         int
	hearbeatTick          int
	Tick                  func()
	HandleMsg             func(*pb.RaftMessage)
	Msg                   []*pb.RaftMessage
	logger                *zap.SugaredLogger
}

func (r *Raft) SwitchCandidate() {
	// r.mu.Lock()
	// defer r.mu.Unlock()

	r.state = CANDIDATE_STATE
	r.leader = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.HandleMsg = r.HandleCandidateMessage

	lastIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	for _, rp := range r.replica {
		rp.NextIndex = lastIndex + 1
		rp.MatchIndex = lastIndex
	}

	r.BroadcastRequestVote()
	r.logger.Debugf("成为 Candidate, 任期: %d", r.currentTerm)
}

func (r *Raft) SwitchFollower(leaderId, term uint64) {
	// r.mu.Lock()
	// defer r.mu.Unlock()

	r.logger.Debugf("成为 Follower, Leader: %d, 任期: %d", leaderId, term)

	r.state = FOLLOWER_STATE
	r.leader = leaderId
	r.currentTerm = term
	r.voteFor = 0
	r.voteResult = make(map[uint64]bool)
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Tick = r.TickElection
	r.HandleMsg = r.HandleFollowerMessage
}

func (r *Raft) SwitchLeader() {
	// r.mu.Lock()
	// defer r.mu.Unlock()

	r.logger.Debugf("成为 Leader, 任期: %d", r.currentTerm)

	r.state = LEADER_STATE
	r.leader = r.id
	r.voteFor = 0
	r.voteResult = make(map[uint64]bool)
	r.Tick = r.TickHeartbeat
	r.HandleMsg = r.HandleLeaderMessage
	r.BroadcastHeartbeat()
}

func (r *Raft) TickHeartbeat() {
	r.hearbeatTick++

	if r.hearbeatTick >= r.heartbeatTimeout {
		r.hearbeatTick = 0
		r.BroadcastHeartbeat()
		for sid, rp := range r.replica {
			if sid == r.id {
				continue
			}
			b := rp.CanSend()

			// size := len(rp.pending)
			// if size > 0 {
			// 	r.logger.Debugf("节点 %d消息发送进度, 活跃状态: %t 未确认：%d~%d ", sid, rp.active, rp.pending[0], rp.pending[size-1])
			// }

			if !b {
				rp.pending = nil
				r.SendAppendEntries(sid)
			}
		}
	}

}

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

func (r *Raft) HandleMessage(msg *pb.RaftMessage) {

	if msg.Term < r.currentTerm {
		return
	}

}

func (r *Raft) HandleCandidateMessage(msg *pb.RaftMessage) {

	// r.logger.Debugf("收到 %s ", msg.String())

	switch msg.MsgType {
	case pb.MessageType_VOTE:
		b := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.electtionTick = 0
		}
	case pb.MessageType_VOTE_RES:
		r.ReciveVoteResult(msg.From, msg.Term, msg.LastLogIndex, msg.LastLogTerm, msg.Success)
	case pb.MessageType_HEARTBEAT:
		b := r.ReciveHeartbeat(msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit)
		if b {
			// r.logger.Debugf("收到心跳,切换为Follower")
			r.SwitchFollower(msg.From, msg.Term)
		}
		r.SendMessage(pb.MessageType_HEARTBEAT_RESP, msg.From, 0, 0, 0, nil, b)
	case pb.MessageType_APPEND_ENTRY:
		b := r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		if b {
			// r.logger.Debugf("收到日志,切换为Follower")
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

func (r *Raft) HandleFollowerMessage(msg *pb.RaftMessage) {

	// r.logger.Debugf("收到 %s ", msg.String())

	switch msg.MsgType {
	case pb.MessageType_VOTE:
		b := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.electtionTick = 0
		}
		return
	case pb.MessageType_HEARTBEAT:
		b := r.ReciveHeartbeat(msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit)
		if b {
			r.electtionTick = 0
		}
		r.SendMessage(pb.MessageType_HEARTBEAT_RESP, msg.From, 0, 0, 0, nil, b)
	case pb.MessageType_APPEND_ENTRY:
		b := r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		if b {
			r.electtionTick = 0
		}
	case pb.MessageType_PROPOSE:
		r.SendMessage(pb.MessageType_PROPOSE, r.leader, 0, 0, 0, msg.Entry, false)
	case pb.MessageType_PROPOSE_RESP:
	default:
		m, _ := json.Marshal(msg)
		r.logger.Debugf("收到异常消息 %s", string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}
}

func (r *Raft) HandleLeaderMessage(msg *pb.RaftMessage) {

	// r.logger.Debugf("收到 %s ", msg.String())

	switch msg.MsgType {
	case pb.MessageType_PROPOSE:
		r.AppendEntry(msg.Entry)
	case pb.MessageType_VOTE:
		r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
	case pb.MessageType_VOTE_RES:
	case pb.MessageType_HEARTBEAT_RESP:
	case pb.MessageType_APPEND_ENTRY_RESP:
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	default:
		m, _ := json.Marshal(msg)
		r.logger.Infof("收到异常消息 %s", string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}
}

func (r *Raft) AppendEntry(entries []*pb.LogEntry) {
	// r.mu.Lock()
	// defer r.mu.Unlock()

	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	for i, entry := range entries {
		entry.Index = lastLogIndex + 1 + uint64(i)
		entry.Term = r.currentTerm
	}

	r.raftlog.AppendEntry(entries)
	r.replica[r.id].NextIndex = lastLogIndex + uint64(len(entries))
	r.replica[r.id].MatchIndex = lastLogIndex + uint64(len(entries)) - 1

	r.BroadcastAppendEntries()

}

func (r *Raft) BroadcastAppendEntries() {
	for id := range r.replica {
		if id == r.id {
			continue
		}
		r.SendAppendEntries(id)
	}
}

func (r *Raft) BroadcastHeartbeat() {
	for id, rep := range r.replica {
		if id == r.id {
			continue
		}
		lastLogIndex := rep.NextIndex - 1
		lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
		r.SendMessage(pb.MessageType_HEARTBEAT, id, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, nil, false)
	}
}

func (r *Raft) BroadcastRequestVote() {
	// r.mu.Lock()
	// defer r.mu.Unlock()

	r.currentTerm++
	r.voteFor = r.id
	r.voteResult = make(map[uint64]bool)
	r.voteResult[r.id] = true

	for id := range r.replica {
		if id == r.id {
			continue
		}
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		r.SendMessage(pb.MessageType_VOTE, id, lastLogIndex, lastLogTerm, 0, nil, false)
	}
}

func (r *Raft) SendAppendEntries(to uint64) {

	b := r.replica[to].CanSend()
	if !b {
		// r.logger.Debugf("节点 %d 停止发送消息, 活跃状态: %t ,未确认消息 %d ", to, r.replica[to].active, len(r.replica[to].pending))
		return
	}

	nextIndex := r.replica[to].NextIndex
	lastLogIndex := nextIndex - 1
	lastLogTerm := r.raftlog.GetTerm(lastLogIndex)

	// var entries []*pb.LogEntry
	entries := r.raftlog.GetEntries(nextIndex)
	size := len(entries)
	if size == 0 {
		// r.logger.Debugf("发送日志到 %d, 范围 %d ~ %d ,nextIndex: %d", to, entries[0].Index, entries[size-1].Index, nextIndex)
		return
	}

	r.replica[to].AppendEntry(entries[size-1].Index)
	r.SendMessage(pb.MessageType_APPEND_ENTRY, to, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, entries, false)

}

func (r *Raft) SendMessage(msgType pb.MessageType, to, lastLogIndex, lastLogTerm, LastCommit uint64, entry []*pb.LogEntry, success bool) {

	// r.logger.Debugf("发送: %s 到: %d", msgType.string(), to)
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

func (r *Raft) ReciveVoteResult(from, term, lastLogIndex, lastLogTerm uint64, success bool) {

	r.voteResult[from] = success
	r.replica[from].Reset(lastLogIndex)

	// r.logger.Debugf("更新节点 %d, 最新下次发送日志为%d", from, lastLogIndex+1)

	granted := 0
	reject := 0
	for _, v := range r.voteResult {
		if v {
			granted++
		} else {
			reject++
		}
	}
	most := len(r.replica)/2 + 1

	r.logger.Debugf("节点发起投票结果, 接受: %d ,拒绝: %d , 法定数量: %d", granted, reject, most)
	if granted >= most {
		r.SwitchLeader()
	} else if reject >= most {
		r.voteFor = 0
		r.voteResult = make(map[uint64]bool)
	}
}

func (r *Raft) ReciveAppendEntriesResult(from, term, lastLogIndex uint64, success bool) {
	// r.mu.Lock()
	// defer r.mu.Unlock()

	if success {
		r.replica[from].AppendEntryResp(lastLogIndex)
		if lastLogIndex > r.raftlog.commitIndex {
			// 取已同步索引更新到lastcommit
			logCount := 0
			for _, rep := range r.replica {
				if lastLogIndex <= rep.MatchIndex {
					logCount++
				}
			}
			if logCount >= len(r.replica)/2+1 {
				r.raftlog.Apply(lastLogIndex, lastLogIndex)
				r.SendAppendEntries(from)
			}
		}
	} else {
		r.logger.Infof("节点 %d 追加日志失败, Leader记录节点最新日志: %d ,节点最新日志: %d ", from, r.replica[from].NextIndex-1, lastLogIndex)

		r.replica[from].Reset(lastLogIndex)
		r.SendAppendEntries(from)
	}
}

func (r *Raft) ReciveHeartbeat(mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64) bool {
	flag := mTerm >= r.currentTerm
	if flag {
		lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
		r.raftlog.Apply(mLastCommit, lastLogIndex)
	}
	return flag
}

func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*pb.LogEntry) (success bool) {
	// r.mu.Lock()
	// defer r.mu.Unlock()

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

	r.logger.Debugf("候选人: %d, 投票: %t ,任期: %d ,最新日志: %d, 最新日志任期: %d, 节点最新日志: %d ,节点最新日志任期： %d ", mCandidateId, success, mTerm, mLastLogIndex, mLastLogTerm, lastLogIndex, lastLogTerm)
	r.SendMessage(pb.MessageType_VOTE_RES, mCandidateId, lastLogIndex, lastLogTerm, 0, nil, success)
	return
}

func NewRaft(id uint64, storage Storage, peers []uint64, logger *zap.SugaredLogger) *Raft {
	if logger == nil {
		zapLogger, _ := zap.NewDevelopment()
		logger = zapLogger.Sugar()
	}

	raftlog := NewRaftLog(storage, logger)

	replica := make(map[uint64]*ReplicaProgress)
	for _, sid := range peers {
		replica[sid] = &ReplicaProgress{
			NextIndex:  raftlog.commitIndex + 1,
			MatchIndex: raftlog.commitIndex,
		}
	}

	raft := &Raft{
		id:               id,
		currentTerm:      raftlog.lastAppliedTerm,
		raftlog:          raftlog,
		replica:          replica,
		electionTimeout:  10,
		heartbeatTimeout: 5,
		logger:           logger,
	}

	logger.Infof("raft初始化,id: %d,任期: %d,最后提交: %d", raft.id, raft.currentTerm, raft.raftlog.commitIndex)
	raft.SwitchFollower(0, raft.currentTerm)
	return raft
}

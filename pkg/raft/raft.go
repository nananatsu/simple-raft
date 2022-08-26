package raft

import (
	"encoding/json"
	"math/rand"
	"sync"

	"go.uber.org/zap"
)

// raft节点类型

type RaftState int

const (
	CANDIDATE_STATE RaftState = iota
	FOLLOWER_STATE
	LEADER_STATE
)

// func (rs RaftState) string() string {
// 	switch rs {
// 	case CANDIDATE_STATE:
// 		return "CANDIDATE"
// 	case FOLLOWER_STATE:
// 		return "FOLLOWER"
// 	case LEADER_STATE:
// 		return "LEADER"
// 	default:
// 		return ""
// 	}
// }

type Peer struct {
	Id    uint64
	Recvc chan Message
}

type Raft struct {
	mu sync.Mutex

	id          uint64
	state       RaftState
	leader      uint64
	currentTerm uint64
	voteFor     uint64
	voteResult  map[uint64]bool

	raftlog *RaftLog

	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	recvc chan Message
	peers map[uint64]*Peer

	electionTimeout  int
	heartbeatTimeout int
	ticker           *Ticker

	logger *zap.SugaredLogger
}

func (r *Raft) Start() {

	// r.SwitchFollower(-1)
	r.SwitchCandidate()
	for {
		switch r.state {
		case CANDIDATE_STATE:
			select {
			case <-r.ticker.Tick:
				r.BroadcastRequestVote()
			case msg := <-r.recvc:
				r.HandleCandidateMessage(&msg)
			}
		case FOLLOWER_STATE:
			select {
			case <-r.ticker.Tick:
				r.SwitchCandidate()
			case msg := <-r.recvc:
				r.HandleFollowerMessage(&msg)
			}
		case LEADER_STATE:
			select {
			case <-r.ticker.Tick:
				r.BroadcastHeartbeat()
			case msg := <-r.recvc:
				r.HandleLeaderMessage(&msg)
			}
		}
	}
}

func (r *Raft) SwitchCandidate() {

	r.logger.Debugf("成为 Candidate, 任期: %d", r.currentTerm)

	r.state = CANDIDATE_STATE
	r.leader = 0
	r.BroadcastRequestVote()

	r.ticker.SetTimeout(r.electionTimeout)
}

func (r *Raft) SwitchFollower(leaderId, term uint64) {

	r.logger.Debugf("成为 Follower, Leader: %d, 任期: %d", leaderId, r.currentTerm)

	r.state = FOLLOWER_STATE

	r.leader = leaderId
	r.currentTerm = term
	r.voteFor = 0
	r.voteResult = make(map[uint64]bool)

	r.ticker.SetTimeout(r.electionTimeout)
}

func (r *Raft) SwitchLeader() {

	r.logger.Debugf("成为 Leader, 任期: %d", r.currentTerm)

	r.state = LEADER_STATE
	r.leader = r.id
	r.voteFor = 0
	r.voteResult = make(map[uint64]bool)

	r.BroadcastHeartbeat()
	r.ticker.SetTimeout(r.heartbeatTimeout)

}

func (r *Raft) HandleCandidateMessage(msg *Message) {

	// r.logger.Debugf("收到 %s ", msg.String())

	switch msg.MsgType {
	case MSG_VOTE:
		b := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.ticker.Reset()
		}
	case MSG_VOTE_RES:
		r.ReciveVoteResult(msg.From, msg.Term, msg.Success)
	case MSG_HEARTBEAT:
		b := r.ReciveHeartbeat(msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit)
		if b {
			r.SwitchFollower(msg.From, msg.Term)
		}
		r.SendMessage(MSG_HEARTBEAT_RESP, msg.From, 0, 0, 0, nil, b)
	case MSG_APPEND_ENTRY:
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

func (r *Raft) HandleFollowerMessage(msg *Message) {

	// r.logger.Debugf("收到 %s ", msg.String())

	switch msg.MsgType {
	case MSG_VOTE:
		b := r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.ticker.Reset()
		}
	case MSG_HEARTBEAT:
		b := r.ReciveHeartbeat(msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit)
		if b {
			r.ticker.Reset()
		}
		r.SendMessage(MSG_HEARTBEAT_RESP, msg.From, 0, 0, 0, nil, b)
	case MSG_APPEND_ENTRY:
		b := r.ReciveAppendEntries(msg.From, msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		if b {
			r.ticker.Reset()
		}
	case MSG_PROPOSE:
		r.SendMessage(MSG_PROPOSE, r.leader, 0, 0, 0, msg.Entry, false)
	case MSG_PROPOSE_RESP:
	default:
		m, _ := json.Marshal(msg)
		r.logger.Debugf("收到异常消息 %s", string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}
}

func (r *Raft) HandleLeaderMessage(msg *Message) {

	// r.logger.Debugf("收到 %s ", msg.String())

	switch msg.MsgType {
	case MSG_PROPOSE:
		r.AppendEntry(msg.Entry)
	case MSG_VOTE:
		r.ReciveRequestVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
	case MSG_HEARTBEAT_RESP:
	case MSG_APPEND_ENTRY_RESP:
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.LastLogIndex, msg.Success)
	default:
		m, _ := json.Marshal(msg)
		r.logger.Infof("收到异常消息 %s", string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}
}

func (r *Raft) Propose(data []byte) {

	r.logger.Debugf("提议: %s", string(data))

	msg := &Message{
		MsgType: MSG_PROPOSE,
		Entry:   []*LogEntry{{Data: data}},
	}
	r.recvc <- *msg
}

func (r *Raft) AppendEntry(entries []*LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lastLogIndex, _ := r.raftlog.GetLastLogIndexAndTerm()
	for i, entry := range entries {
		entry.Index = lastLogIndex + 1 + uint64(i)
		entry.Term = r.currentTerm
	}

	r.logger.Debugf("添加日志: %d - %d", entries[0].Index, entries[len(entries)-1].Index)
	r.raftlog.AppendEntry(entries)
	r.nextIndex[r.id] = lastLogIndex + uint64(len(entries))
	r.matchIndex[r.id] = lastLogIndex + uint64(len(entries)) - 1

	r.BroadcastAppendEntries(false)

}

func (r *Raft) BroadcastAppendEntries(emptyEntry bool) {
	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		r.SendAppendEntries(peer.Id, emptyEntry)
	}
}

func (r *Raft) BroadcastHeartbeat() {
	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		lastLogIndex := r.matchIndex[peer.Id]
		lastLogTerm := r.raftlog.GetTerm(lastLogIndex)
		r.SendMessage(MSG_HEARTBEAT, peer.Id, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, nil, false)
	}
}

func (r *Raft) BroadcastRequestVote() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm++
	r.voteFor = r.id
	r.voteResult = make(map[uint64]bool)

	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		r.SendMessage(MSG_VOTE, peer.Id, lastLogIndex, lastLogTerm, 0, nil, false)
	}
}

func (r *Raft) SendAppendEntries(to uint64, emptyEntry bool) {

	lastLogIndex := r.matchIndex[to]
	lastLogTerm := r.raftlog.GetTerm(lastLogIndex)

	var entries []*LogEntry
	if !emptyEntry {
		nextIndex := r.nextIndex[to]
		entries = r.raftlog.GetEntries(nextIndex)

		size := len(entries)
		if size == 0 {
			r.logger.Errorf("取得待同步日志失败")
			return
		}
		r.nextIndex[to] += uint64(size)

	}

	r.SendMessage(MSG_APPEND_ENTRY, to, lastLogIndex, lastLogTerm, r.raftlog.commitIndex, entries, false)

}

func (r *Raft) SendMessage(msgType RaftMsgType, to, lastLogIndex, lastLogTerm, LastCommit uint64, entry []*LogEntry, success bool) {

	// r.logger.Debugf("发送: %s 到: %d", msgType.string(), to)

	toPeer := r.peers[to]
	if toPeer != nil {
		toPeer.Recvc <- Message{
			MsgType:      msgType,
			Term:         r.currentTerm,
			From:         r.id,
			To:           toPeer.Id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
			LastCommit:   r.raftlog.commitIndex,
			Entry:        entry,
			Success:      success,
		}
	}
}

func (r *Raft) ReciveVoteResult(from, term uint64, success bool) {

	r.voteResult[from] = success
	granted := 0
	reject := 0
	for _, v := range r.voteResult {
		if v {
			granted++
		} else {
			reject++
		}
	}
	most := len(r.peers)/2 + 1

	if granted >= most {
		r.SwitchLeader()
	} else if reject >= most {
		r.voteFor = 0
		r.voteResult = make(map[uint64]bool)
	}
}

func (r *Raft) ReciveAppendEntriesResult(from, term, lastLogIndex uint64, success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentTerm == term {
		r.matchIndex[from] = lastLogIndex
		r.nextIndex[from] = lastLogIndex + 1
	}

	if success {
		if lastLogIndex > r.raftlog.commitIndex {
			// 取已同步索引更新到lastcommit
			logCount := 0
			for _, index := range r.matchIndex {
				if lastLogIndex >= index {
					logCount++
				}
			}
			if logCount >= len(r.peers)/2+1 && r.raftlog.commitIndex < lastLogIndex {
				r.raftlog.Apply(lastLogIndex, lastLogIndex)

				// 发送心跳以更新lastcommit
				r.BroadcastHeartbeat()
			}
		}
	} else {
		r.logger.Debugf("节点 %d 追加日志失败", from)
		r.SendAppendEntries(from, false)
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

func (r *Raft) ReciveAppendEntries(mLeader, mTerm, mLastLogTerm, mLastLogIndex, mLastCommit uint64, mEntries []*LogEntry) (success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if mTerm < r.currentTerm {
		r.logger.Infof("消息 %s 任期%d 小于当前任期 %d", mTerm, r.currentTerm)
		success = false
	} else if !r.raftlog.HasPrevLog(mLastLogIndex, mLastLogTerm) { // 检查节点是否拥有leader最后提交日志

		r.logger.Infof("节点未含有上次追加日志: Index: %d, Term: %d", mLastLogIndex, mLastLogTerm)
		success = false
	} else {
		r.logger.Infof("追加日志数量 %d", len(mEntries))
		r.raftlog.RemoveConflictLog(mEntries)
		r.raftlog.AppendEntry(mEntries)
		success = true
	}

	lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
	r.raftlog.Apply(mLastCommit, lastLogIndex)

	r.SendMessage(MSG_APPEND_ENTRY_RESP, mLeader, lastLogIndex, lastLogTerm, 0, nil, success)
	return
}

func (r *Raft) ReciveRequestVote(mTerm, mCandidateId, mLastLogTerm, mLastLogIndex uint64) (success bool) {
	if mTerm < r.currentTerm {
		success = false
	} else if r.voteFor == 0 || r.voteFor == mCandidateId {
		lastLogIndex, lastLogTerm := r.raftlog.GetLastLogIndexAndTerm()
		if mLastLogTerm >= lastLogTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			success = true
		}
	}

	r.SendMessage(MSG_VOTE_RES, mCandidateId, 0, 0, 0, nil, success)
	return
}

func NewRaft(id uint64, recvc chan Message, storage Storage, pees []*Peer, logger *zap.SugaredLogger) *Raft {

	if logger == nil {
		logger = zap.L().Sugar()
	}

	nextIndex := make(map[uint64]uint64)
	matchIndex := make(map[uint64]uint64)
	peerMap := make(map[uint64]*Peer)
	for _, p := range pees {
		peerMap[p.Id] = p
		nextIndex[p.Id] = 1
		matchIndex[p.Id] = 0
	}

	raft := &Raft{
		id:               id,
		raftlog:          NewRaftLog(storage, logger),
		nextIndex:        nextIndex,
		matchIndex:       make(map[uint64]uint64),
		recvc:            recvc,
		peers:            peerMap,
		electionTimeout:  10 + rand.Intn(10),
		heartbeatTimeout: 5,
		ticker:           NewTicker(),
		logger:           logger,
	}

	go raft.Start()

	return raft
}

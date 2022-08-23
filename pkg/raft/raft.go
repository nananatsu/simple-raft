package raft

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
)

// raft节点类型
const (
	CANDIDATE_STATE = iota
	FOLLOWER_STATE
	LEADER_STATE
)

// 消息类型
const (
	MSG_VOTE = iota
	MSG_VOTE_RESULT
	MSG_APPEND_ENTRY
	MSG_APPEND_ENTRY_RESULT
	MSG_PROPOSE
	MSG_PROPOSE_RESULT
)

type Snapshot struct {
	Offset int64
	Data   []byte
	Done   bool
}

type Message struct {
	MsgType      int
	Term         int64
	From         int64
	To           int64
	LastLogIndex int64
	LastLogTerm  int64
	LastCommit   int64
	Entry        []*LogEntry
	Snapshot     *Snapshot
	Success      bool
}

type Peer struct {
	Id    int64
	Recvc chan Message
}

type Raft struct {
	mu sync.Mutex

	id          int64
	state       int
	leader      int64
	currentTerm int64
	voteFor     int64
	voteResult  map[int64]bool

	log *Log

	nextIndex  map[int64]int64
	matchIndex map[int64]int64

	recvc chan Message
	peers map[int64]*Peer

	electionTimeout  int
	heartbeatTimeout int
	ticker           *Ticker
}

func (r *Raft) Start() {

	// r.SwitchFollower(0)
	r.SwitchCandidate()
	for {
		switch r.state {
		case CANDIDATE_STATE:
			select {
			case <-r.ticker.Tick:
				r.Elect()
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

	log.Printf("节点 %d 成为 Candidate, 任期：%d \n", r.id, r.currentTerm)

	r.state = CANDIDATE_STATE
	r.Elect()

	r.ticker.SetTimeout(r.electionTimeout)

}

func (r *Raft) SwitchFollower(leaderId, term int64) {

	log.Printf("节点 %d 成为 Follower, Leader: %d, 任期：%d \n", r.id, leaderId, term)

	r.state = FOLLOWER_STATE

	r.leader = leaderId
	r.currentTerm = term
	r.voteFor = -1
	r.voteResult = make(map[int64]bool)

	r.ticker.SetTimeout(r.electionTimeout)
}

func (r *Raft) SwitchLeader() {

	log.Printf("节点 %d 成为 Leader, 任期：%d \n", r.id, r.currentTerm)

	r.state = LEADER_STATE
	r.leader = r.id
	r.voteFor = -1
	r.voteResult = make(map[int64]bool)

	r.BroadcastHeartbeat()
	r.ticker.SetTimeout(r.heartbeatTimeout)

}

func (r *Raft) HandleCandidateMessage(msg *Message) {

	switch msg.MsgType {
	case MSG_VOTE:
		b := r.ReciveVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.ticker.Reset()
		}
		r.SendReply(MSG_VOTE_RESULT, msg.From, b)
	case MSG_VOTE_RESULT:
		r.ReciveVoteResult(msg.From, msg.Term, msg.Success)
	case MSG_APPEND_ENTRY:
		b := r.ReciveAppendEntries(msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		if b {
			r.SwitchFollower(msg.From, msg.Term)
		}
		r.SendReply(MSG_APPEND_ENTRY_RESULT, msg.From, b)
	default:
		m, _ := json.Marshal(msg)
		log.Printf("节点 %d 收到异常消息 %v", r.id, string(m))
	}

	// if msg.Term > r.currentTerm {
	// 	r.SwitchFollower(msg.From, msg.Term)
	// }

}

func (r *Raft) HandleFollowerMessage(msg *Message) {

	switch msg.MsgType {
	case MSG_VOTE:
		b := r.ReciveVote(msg.Term, msg.From, msg.LastLogTerm, msg.LastLogIndex)
		if b {
			r.ticker.Reset()
		}
		r.SendReply(MSG_VOTE_RESULT, msg.From, b)
	case MSG_APPEND_ENTRY:
		b := r.ReciveAppendEntries(msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		if b {
			r.ticker.Reset()
		}
		r.SendReply(MSG_APPEND_ENTRY_RESULT, msg.From, b)
	case MSG_PROPOSE_RESULT:
	default:
		m, _ := json.Marshal(msg)
		log.Printf("节点 %d 收到异常消息 %v", r.id, string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}
}

func (r *Raft) HandleLeaderMessage(msg *Message) {
	switch msg.MsgType {
	case MSG_PROPOSE:
	case MSG_VOTE:
		b := r.ReciveAppendEntries(msg.Term, msg.LastLogTerm, msg.LastLogIndex, msg.LastCommit, msg.Entry)
		r.SendReply(MSG_VOTE_RESULT, msg.From, b)
	case MSG_APPEND_ENTRY_RESULT:
		r.ReciveAppendEntriesResult(msg.From, msg.Term, msg.Success)
	default:
		m, _ := json.Marshal(msg)
		log.Printf("节点 %d 收到异常消息 %v", r.id, string(m))
	}

	if msg.Term > r.currentTerm {
		r.SwitchFollower(msg.From, msg.Term)
	}

}

func (r *Raft) Propose(data []byte) {
	msg := &Message{
		MsgType: MSG_PROPOSE,
		Entry:   []*LogEntry{{Data: data}},
	}
	r.recvc <- *msg
}

func (r *Raft) AppendEntry(entries []*LogEntry) {

	lastLogIndex, _ := r.log.getLastLogTermAndIndex()
	for i, entry := range entries {
		entry.Index = lastLogIndex + 1 + int64(i)
		entry.Term = r.currentTerm
	}
	r.log.AppendEntry(entries)

}

func (r *Raft) BroadcastHeartbeat() {
	if r.state == LEADER_STATE {
		r.mu.Lock()
		defer r.mu.Unlock()

		for _, peer := range r.peers {
			if peer.Id == r.id {
				continue
			}
			r.SendMessage(MSG_APPEND_ENTRY, peer.Id, nil)
		}
	}
}

func (r *Raft) Elect() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm++
	r.voteFor = r.id
	r.voteResult = make(map[int64]bool)

	for _, peer := range r.peers {
		if peer.Id == r.id {
			continue
		}
		r.SendMessage(MSG_VOTE, peer.Id, nil)
	}
}

func (r *Raft) SendReply(msgType int, to int64, success bool) {
	toPeer := r.peers[to]

	if toPeer != nil {
		toPeer.Recvc <- Message{
			MsgType: msgType,
			Term:    r.currentTerm,
			From:    r.id,
			To:      toPeer.Id,
			Success: success,
		}
	}
}

func (r *Raft) SendMessage(msgType int, to int64, entry []*LogEntry) {

	log.Printf("节点 %d 发送消息 %d 到 %d", r.id, msgType, to)

	toPeer := r.peers[to]
	lastLogIndex, lastLogTerm := r.log.getLastLogTermAndIndex()

	if toPeer != nil {
		toPeer.Recvc <- Message{
			MsgType:      msgType,
			Term:         r.currentTerm,
			From:         r.id,
			To:           toPeer.Id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
			LastCommit:   r.log.commitIndex,
			Entry:        entry,
		}
	}
}

func (r *Raft) ReciveVoteResult(from, term int64, success bool) {
	log.Printf("节点 %d 收到选取响应，结果： %t, 任期：%d \n", r.id, success, term)

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
		r.voteFor = -1
		r.voteResult = make(map[int64]bool)
	}
}

func (r *Raft) ReciveAppendEntriesResult(from, term int64, success bool) {
	log.Printf("节点 %d 收到日志追加响应，结果： %t, 任期：%d \n", r.id, success, term)
	if success && r.currentTerm == term {
		r.matchIndex[from] = r.nextIndex[from]
		r.nextIndex[from]++

		// 取已同步索引更新到lastcommit
	}
}

func (r *Raft) ReciveAppendEntries(mTerm, mLastLogTerm, mLastLogIndex, mLastCommit int64, mEntries []*LogEntry) bool {
	log.Printf("节点 %d 收到日志追加请求， 任期：%d \n", r.id, mTerm)

	if mTerm < r.currentTerm {
		return false
	}

	// 心跳
	if len(mEntries) == 0 {
		return true
	}

	// 检查节点是否拥有leader最后提交日志
	if !r.log.HasPrevLog(mLastLogIndex, mLastLogTerm) {
		return false
	}

	r.log.RemoveConflictLog(mEntries[0])
	r.log.AppendEntry(mEntries)

	return true
}

func (r *Raft) ReciveVote(mTerm, mCandidateId, mLastLogTerm, mLastLogIndex int64) bool {

	log.Printf("节点 %d 收到选取请求，候选人： %d, 任期：%d \n", r.id, mCandidateId, mTerm)
	if mTerm < r.currentTerm {
		return false
	}

	if r.voteFor == -1 || r.voteFor == mCandidateId {
		lastLogIndex, lastLogTerm := r.log.getLastLogTermAndIndex()
		if mLastLogTerm >= lastLogTerm && mLastLogIndex >= lastLogIndex {
			r.voteFor = mCandidateId
			return true
		}
	}
	return false
}

func NewRaft(id int64, recvc chan Message, storage Storage, pees []*Peer) *Raft {

	peerMap := make(map[int64]*Peer)
	for _, p := range pees {
		peerMap[p.Id] = p
	}

	raft := &Raft{
		id:               id,
		log:              NewLog(storage),
		nextIndex:        make(map[int64]int64),
		matchIndex:       make(map[int64]int64),
		recvc:            recvc,
		peers:            peerMap,
		electionTimeout:  10 + rand.Intn(10),
		heartbeatTimeout: 5,
		ticker:           NewTicker(),
	}

	go raft.Start()

	return raft
}

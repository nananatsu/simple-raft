package raft

import (
	pb "kvdb/pkg/raftpb"
	"time"

	"go.uber.org/zap"
)

type RaftStatus struct {
	PendingLogSize int
	AppliedLogSize uint64
}

type RaftNode struct {
	raft    *Raft
	recvc   chan *pb.RaftMessage
	propc   chan *pb.RaftMessage
	sendc   chan []*pb.RaftMessage
	changec chan []*pb.MemberChange
	stopc   chan struct{}
	ticker  *time.Ticker
	logger  *zap.SugaredLogger
}

func (n *RaftNode) Start() {
	go func() {
		var propc chan *pb.RaftMessage
		var sendc chan []*pb.RaftMessage
		for {
			var msgs []*pb.RaftMessage
			if len(n.raft.Msg) > 0 {
				msgs = n.raft.Msg
				sendc = n.sendc
			} else {
				sendc = nil
			}

			if len(n.recvc) > 0 || len(sendc) > 0 {
				propc = nil
			} else {
				propc = n.propc
			}

			select {
			case <-n.ticker.C:
				n.raft.Tick()
			case msg := <-n.recvc:
				n.raft.HandleMessage(msg)
			case msg := <-propc:
				n.raft.HandleMessage(msg)
			case sendc <- msgs:
				n.raft.Msg = nil
			case changes := <-n.changec:
				n.raft.ChangeMember(changes)
			case <-n.stopc:
				return
			}
		}
	}()
}

func (n *RaftNode) HandlePropose(msg *pb.RaftMessage) {
	n.propc <- msg
}

func (n *RaftNode) Propose(entries []*pb.LogEntry) {
	msg := &pb.RaftMessage{
		MsgType: pb.MessageType_PROPOSE,
		Term:    n.raft.currentTerm,
		Entry:   entries,
	}
	n.HandlePropose(msg)
}

func (n *RaftNode) ChangeMember(changes []*pb.MemberChange) {
	n.changec <- changes
}

func (n *RaftNode) HandleMsg(msg *pb.RaftMessage) {
	n.recvc <- msg
}

func (n *RaftNode) Poll() chan []*pb.RaftMessage {
	return n.sendc
}

func (n *RaftNode) Ready() bool {
	if n.raft.cluster.pendingChangeIndex <= n.raft.raftlog.lastAppliedIndex {
		return n.raft.state == LEADER_STATE || (n.raft.state == FOLLOWER_STATE && n.raft.leader != 0)
	}
	return false
}

func (n *RaftNode) IsLeader() bool {
	return n.raft.state == LEADER_STATE
}

func (n *RaftNode) Status() *RaftStatus {
	return &RaftStatus{
		PendingLogSize: len(n.raft.raftlog.logs),
		AppliedLogSize: n.raft.raftlog.lastAppliedIndex,
	}
}

func (n *RaftNode) Close() {
	n.stopc <- struct{}{}
	close(n.recvc)
	close(n.propc)
	close(n.sendc)
	close(n.changec)
	close(n.stopc)
	n.ticker.Stop()
}

func NewRaftNode(id uint64, storage Storage, peers []uint64, logger *zap.SugaredLogger) *RaftNode {

	node := &RaftNode{
		raft:    NewRaft(id, storage, peers, logger),
		recvc:   make(chan *pb.RaftMessage),
		propc:   make(chan *pb.RaftMessage),
		sendc:   make(chan []*pb.RaftMessage),
		changec: make(chan []*pb.MemberChange),
		stopc:   make(chan struct{}),
		ticker:  time.NewTicker(time.Second),
		logger:  logger,
	}

	node.Start()

	return node
}

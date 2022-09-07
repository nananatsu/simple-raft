package raft

import (
	pb "kvdb/pkg/raftpb"
	"time"

	"go.uber.org/zap"
)

type RaftStatus struct {
	RecvcSize      int
	PropcSize      int
	SendcSize      int
	PendingLogSize int
	AppliedLogSize uint64
}

type RaftNode struct {
	raft   *Raft
	Recvc  chan *pb.RaftMessage
	Propc  chan *pb.RaftMessage
	Sendc  chan []*pb.RaftMessage
	ticker *time.Ticker
	logger *zap.SugaredLogger
}

func (n *RaftNode) Start() {
	go func() {
		var propc chan *pb.RaftMessage
		var sendc chan []*pb.RaftMessage
		for {
			var msgs []*pb.RaftMessage
			if len(n.raft.Msg) > 0 {
				msgs = n.raft.Msg
				sendc = n.Sendc
			} else {
				sendc = nil
			}

			if len(n.Recvc) > 0 || len(sendc) > 0 {
				propc = nil
			} else {
				propc = n.Propc
			}

			select {
			case <-n.ticker.C:
				n.raft.Tick()
			case msg := <-n.Recvc:
				n.raft.HandleMsg(msg)
			case msg := <-propc:
				n.raft.HandleMsg(msg)
			case sendc <- msgs:
				n.raft.Msg = nil
			}
		}
	}()
}

func (n *RaftNode) HandlePropose(msg *pb.RaftMessage) {
	n.Propc <- msg
}

func (n *RaftNode) Propose(data []byte) {
	msg := &pb.RaftMessage{
		MsgType: pb.MessageType_PROPOSE,
		Term:    n.raft.currentTerm,
		Entry:   []*pb.LogEntry{{Data: data}},
	}
	n.HandlePropose(msg)
}

func (n *RaftNode) HandleMsg(msg *pb.RaftMessage) {
	n.Recvc <- msg
}

func (n *RaftNode) Ready() bool {
	return n.raft.state == LEADER_STATE || (n.raft.state == FOLLOWER_STATE && n.raft.leader != 0)
}

func (n *RaftNode) IsLeader() bool {
	return n.raft.state == LEADER_STATE
}

func (n *RaftNode) Status() *RaftStatus {
	return &RaftStatus{
		RecvcSize:      len(n.Recvc),
		PropcSize:      len(n.Propc),
		SendcSize:      len(n.Sendc),
		PendingLogSize: len(n.raft.raftlog.logs),
		AppliedLogSize: n.raft.raftlog.lastAppliedIndex,
	}
}

func NewRaftNode(id uint64, storage Storage, peers []uint64, logger *zap.SugaredLogger) *RaftNode {

	raft := NewRaft(id, storage, peers, logger)

	propc := make(chan *pb.RaftMessage, 1000)
	recvc := make(chan *pb.RaftMessage, 1000)
	sendc := make(chan []*pb.RaftMessage, 1000)
	ticker := time.NewTicker(time.Second)

	node := &RaftNode{
		raft:   raft,
		Recvc:  recvc,
		Propc:  propc,
		Sendc:  sendc,
		ticker: ticker,
		logger: logger,
	}

	node.Start()

	return node
}

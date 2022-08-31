package raft

import (
	pb "kvdb/pkg/raftpb"
	"time"

	"go.uber.org/zap"
)

type RaftNode struct {
	raft   *Raft
	Recvc  chan *pb.RaftMessage
	Propc  chan *pb.RaftMessage
	Sendc  chan []*pb.RaftMessage
	ticker *time.Ticker

	metric chan pb.MessageType
	logger *zap.SugaredLogger
}

func (n *RaftNode) Start() {

	var sendc chan []*pb.RaftMessage
	for {
		var msgs []*pb.RaftMessage
		if len(n.raft.Msg) > 0 {
			msgs = n.raft.Msg
			sendc = n.Sendc
		} else {
			sendc = nil
		}

		select {
		case <-n.ticker.C:
			n.raft.Tick()
		case msg := <-n.Recvc:
			n.raft.HandleMsg(msg)
		case msg := <-n.Propc:
			n.raft.HandleMsg(msg)
		case sendc <- msgs:
			n.raft.Msg = nil
		}
	}
}

func (n *RaftNode) HandlePropose(msg *pb.RaftMessage) {
	n.metric <- msg.MsgType
	n.Propc <- msg
}

func (n *RaftNode) Propose(data []byte) {
	msg := &pb.RaftMessage{
		MsgType: pb.MessageType_PROPOSE,
		Entry:   []*pb.LogEntry{{Data: data}},
	}
	n.HandlePropose(msg)
}

func (n *RaftNode) HandleMsg(msg *pb.RaftMessage) {
	n.metric <- msg.MsgType
	n.Recvc <- msg
}

func (n *RaftNode) Ready() bool {
	return n.raft.state == LEADER_STATE || (n.raft.state == FOLLOWER_STATE && n.raft.leader != 0)
}

func (n *RaftNode) IsLeader() bool {
	return n.raft.state == LEADER_STATE
}

func (n *RaftNode) showMetrics() {
	voteCount := 0
	voteResCount := 0
	heartbeatCount := 0
	heartbeatResCount := 0
	appEntryCount := 0
	appEntryResCount := 0
	propCount := 0
	propResCOunt := 0

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			<-ticker.C

			switch n.raft.state {
			case LEADER_STATE:
				n.logger.Infof("提议: %d,心跳响应: %d, 日志响应: %d, 已提交: %d,未提交： %d, 消息接收通道: %d, 提议接收通道: %d, 消息发送通道: %d", propCount, heartbeatResCount, appEntryResCount, n.raft.raftlog.lastApplied, len(n.raft.raftlog.logs), len(n.Recvc), len(n.Propc), len(n.Sendc))
			case FOLLOWER_STATE:
				n.logger.Infof("提议: %d,心跳: %d,日志: %d, 已提交: %d,未提交： %d, 消息接收通道: %d, 提议接收通道: %d, 消息发送通道: %d", propCount, heartbeatCount, appEntryCount, n.raft.raftlog.lastApplied, len(n.raft.raftlog.logs), len(n.Recvc), len(n.Propc), len(n.Sendc))
			case CANDIDATE_STATE:
				n.logger.Infof("选取: %d ,选取响应: %d,已提交: %d,未提交： %d, 消息接收通道: %d, 提议接收通道: %d, 消息发送通道: %d", voteCount, voteResCount, n.raft.raftlog.lastApplied, len(n.raft.raftlog.logs), len(n.Recvc), len(n.Propc), len(n.Sendc))
			}
			// n.logger.Infof("已提交: %d,未提交： %d, 消息接收通道: %d, 提议接收通道: %d, 消息发送通道: %d",  appEntryCount, n.raft.raftlog.lastApplied, len(n.raft.raftlog.logs), len(n.Recvc), len(n.Propc), len(n.Sendc))
			// n.logger.Infof("选取: %d ,选取响应: %d,心跳: %d,心跳响应: %d,日志: %d,日志响应: %d,提议: %d,提议响应: %d",
			// 	voteCount, voteResCount, heartbeatCount, heartbeatResCount, appEntryCount, appEntryResCount, propCount, propResCOunt)
		}
	}()

	for {
		t := <-n.metric
		switch t {
		case pb.MessageType_VOTE:
			voteCount++
		case pb.MessageType_VOTE_RES:
			voteResCount++
		case pb.MessageType_HEARTBEAT:
			heartbeatCount++
		case pb.MessageType_HEARTBEAT_RESP:
			heartbeatResCount++
		case pb.MessageType_APPEND_ENTRY:
			appEntryCount++
		case pb.MessageType_APPEND_ENTRY_RESP:
			appEntryResCount++
		case pb.MessageType_PROPOSE:
			propCount++
		case pb.MessageType_PROPOSE_RESP:
			propResCOunt++
		}
	}

}

func NewRaftNode(id uint64, storage Storage, pees []*Peer, logger *zap.SugaredLogger) *RaftNode {

	raft := NewRaft(id, storage, pees, logger)

	propc := make(chan *pb.RaftMessage)
	recvc := make(chan *pb.RaftMessage, 1000)
	sendc := make(chan []*pb.RaftMessage, 1000)
	ticker := time.NewTicker(time.Second)

	metric := make(chan pb.MessageType, 100000)

	node := &RaftNode{
		raft: raft,

		Recvc:  recvc,
		Propc:  propc,
		Sendc:  sendc,
		metric: metric,
		ticker: ticker,
		logger: logger,
	}

	go node.Start()
	go node.showMetrics()

	return node
}

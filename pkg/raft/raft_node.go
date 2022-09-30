package raft

import (
	"fmt"
	pb "kvdb/pkg/raftpb"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// raft节点状态
type RaftStatus struct {
	PendingLogSize int
	AppliedLogSize uint64
}

type RaftNode struct {
	raft    *Raft                   // raft实例
	recvc   chan *pb.RaftMessage    // 一般消息接收通道
	propc   chan *pb.RaftMessage    // 提议消息接收通道
	sendc   chan []*pb.RaftMessage  // 消息发送通道
	changec chan []*pb.MemberChange // 变更接收通道
	stopc   chan struct{}           // 停止
	ticker  *time.Ticker            // 定时器(选取、心跳)
	logger  *zap.SugaredLogger
}

// 启动raft
func (n *RaftNode) Start() {
	go func() {
		var propc chan *pb.RaftMessage
		var sendc chan []*pb.RaftMessage
		for {
			var msgs []*pb.RaftMessage
			// 存在待发送消息，启用发送通道以发送
			if len(n.raft.Msg) > 0 {
				msgs = n.raft.Msg
				sendc = n.sendc
			} else { // 无消息发送隐藏发送通道
				sendc = nil
			}

			// 接收/发送通道存在数据是，不处理提议
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
				n.raft.ApplyChange(changes)
			case <-n.stopc:
				return
			}
		}
	}()
}

// 处理提议
func (n *RaftNode) HandlePropose(msg *pb.RaftMessage) {
	n.propc <- msg
}

// 提议
func (n *RaftNode) Propose(entries []*pb.LogEntry) {
	msg := &pb.RaftMessage{
		MsgType: pb.MessageType_PROPOSE,
		Term:    n.raft.currentTerm,
		Entry:   entries,
	}
	n.HandlePropose(msg)
}

// 集群首次启动时，添加成员到raft日志
func (n *RaftNode) InitMember(peers map[uint64]string) {
	changes := make([]*pb.MemberChange, 0, len(peers))
	for pid, address := range peers {
		changes = append(changes, &pb.MemberChange{
			Type:    pb.MemberChangeType_ADD_NODE,
			Id:      pid,
			Address: address,
		})
	}

	data, err := proto.Marshal(&pb.MemberChangeCollection{Changes: changes})
	if err != nil {
		n.logger.Errorf("序列化集群成员配置为raft日志失败: %v", err)
	} else {
		lastIndex, _ := n.raft.raftlog.GetLastLogIndexAndTerm()
		entry := &pb.LogEntry{
			Type:  pb.EntryType_MEMBER_CHNAGE,
			Term:  n.raft.currentTerm,
			Index: lastIndex + 1,
			Data:  data,
		}
		n.logger.Infof("存储集群成员为raft成员变更日志 %d %d", entry.Index, entry.Term)

		n.raft.raftlog.AppendEntry([]*pb.LogEntry{entry})
		n.raft.cluster.UpdateLogIndex(n.raft.id, entry.Index)
	}
}

// 变更成员提议
func (n *RaftNode) ChangeMember(changes []*pb.MemberChange) error {
	if n.raft.cluster.pendingChangeIndex <= n.raft.raftlog.lastAppliedIndex {
		changeCol := &pb.MemberChangeCollection{Changes: changes}
		data, err := proto.Marshal(changeCol)
		if err != nil {
			n.logger.Errorf("序列化变更成员信息失败: %v", err)
			return err
		}
		n.Propose([]*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE, Data: data}})
	}
	return fmt.Errorf("上次变更未完成")
}

// 变更成员
func (n *RaftNode) ApplyChange(changes []*pb.MemberChange) {
	n.changec <- changes
}

// 处理消息
func (n *RaftNode) HandleMsg(msg *pb.RaftMessage) {
	n.recvc <- msg
}

// 返回发送通道
func (n *RaftNode) Poll() chan []*pb.RaftMessage {
	return n.sendc
}

// 返回通知通道: 成员变更通知
func (n *RaftNode) ChangeNotify() chan []*pb.MemberChange {
	return n.raft.raftlog.storage.Notify()
}

// 节点是否就绪
func (n *RaftNode) Ready() bool {
	if n.raft.cluster.pendingChangeIndex <= n.raft.raftlog.lastAppliedIndex {
		return n.raft.state == LEADER_STATE || (n.raft.state == FOLLOWER_STATE && n.raft.leader != 0)
	}
	return false
}

// 节点是否为Leader
func (n *RaftNode) IsLeader() bool {
	return n.raft.state == LEADER_STATE
}

// 获取Leader
func (n *RaftNode) GetLeader() uint64 {
	return n.raft.leader
}

// 获取当前状态
func (n *RaftNode) Status() *RaftStatus {
	return &RaftStatus{
		PendingLogSize: len(n.raft.raftlog.logEnties),
		AppliedLogSize: n.raft.raftlog.lastAppliedIndex,
	}
}

// 关闭
func (n *RaftNode) Close() {
	n.stopc <- struct{}{}
	close(n.recvc)
	close(n.propc)
	close(n.sendc)
	close(n.changec)
	close(n.stopc)
	n.ticker.Stop()
	n.raft.raftlog.storage.Close()
}

func NewRaftNode(id uint64, storage Storage, peers map[uint64]string, logger *zap.SugaredLogger) *RaftNode {

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

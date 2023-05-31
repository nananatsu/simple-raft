package raft

import (
	"context"
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
	raft       *Raft                  // raft实例
	recvc      chan *pb.RaftMessage   // 一般消息接收通道
	propc      chan *pb.RaftMessage   // 提议消息接收通道
	sendc      chan []*pb.RaftMessage // 消息发送通道
	readIndexc chan *ReadIndexResp
	changec    chan []*pb.MemberChange // 变更接收通道
	stopc      chan struct{}           // 停止
	ticker     *time.Ticker            // 定时器(选取、心跳)
	waitQueue  []*WaitApply
	logger     *zap.SugaredLogger
}

// 启动raft
func (n *RaftNode) Start() {
	go func() {
		var propc chan *pb.RaftMessage
		var sendc chan []*pb.RaftMessage
		var readc chan *ReadIndexResp
		for {
			var msgs []*pb.RaftMessage
			var readIndex *ReadIndexResp
			// 存在待发送消息，启用发送通道以发送
			if len(n.raft.Msg) > 0 {
				msgs = n.raft.Msg
				sendc = n.sendc
			} else { // 无消息发送隐藏发送通道
				sendc = nil
			}

			if len(n.raft.ReadIndex) > 0 {
				readIndex = n.raft.ReadIndex[0]
				readc = n.readIndexc
			} else {
				readc = nil
			}

			// 接收/发送通道存在数据是，不处理提议
			if len(n.recvc) > 0 || len(sendc) > 0 {
				propc = nil
			} else {
				propc = n.propc
			}

			if len(n.waitQueue) > 0 {
				n.raft.raftlog.WaitIndexApply(n.waitQueue)
				n.waitQueue = nil
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
			case readc <- readIndex:
				n.raft.ReadIndex = n.raft.ReadIndex[1:]
			case changes := <-n.changec:
				n.raft.ApplyChange(changes)
			case <-n.stopc:
				return
			}
		}
	}()
}

// 处理消息
func (n *RaftNode) Process(ctx context.Context, msg *pb.RaftMessage) error {
	var ch chan *pb.RaftMessage
	if msg.MsgType == pb.MessageType_PROPOSE {
		ch = n.propc
	} else {
		ch = n.recvc
	}

	select {
	case ch <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *RaftNode) ReadIndex(ctx context.Context, req []byte) error {

	return n.Process(ctx, &pb.RaftMessage{MsgType: pb.MessageType_READINDEX, Term: n.raft.currentTerm, Context: req})
}

func (n *RaftNode) WaitIndexApply(ctx context.Context, index uint64) error {

	ch := make(chan struct{})
	wa := &WaitApply{index: index, ch: ch}
	n.waitQueue = append(n.waitQueue, wa)

	n.logger.Debugf("等待日志 %d 提交", index)
	select {
	case <-ch:
		return nil
	default:
		if n.GetLastAppliedIndex() >= index {
			wa.done = true
			return nil
		}
	}

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		wa.done = true
		if n.GetLastAppliedIndex() >= index {
			return nil
		}
		return fmt.Errorf("等待日志 %d 提交: %v", index, ctx.Err())
	}
}

// 提议
func (n *RaftNode) Propose(ctx context.Context, entries []*pb.LogEntry) error {
	msg := &pb.RaftMessage{
		MsgType: pb.MessageType_PROPOSE,
		Term:    n.raft.currentTerm,
		Entry:   entries,
	}
	return n.Process(ctx, msg)
}

// 变更成员提议
func (n *RaftNode) ChangeMember(ctx context.Context, changes []*pb.MemberChange) error {
	changeCol := &pb.MemberChangeCol{Changes: changes}
	data, err := proto.Marshal(changeCol)
	if err != nil {
		n.logger.Errorf("序列化变更成员信息失败: %v", err)
		return err
	}
	return n.Propose(ctx, []*pb.LogEntry{{Type: pb.EntryType_MEMBER_CHNAGE, Data: data}})
}

// 检查是否能进行变更
func (n *RaftNode) CanChange(changes []*pb.MemberChange) bool {
	return (n.raft.cluster.pendingChangeIndex <= n.raft.raftlog.lastAppliedIndex) && n.raft.cluster.CanChange(changes)
}

// 变更成员
func (n *RaftNode) ApplyChange(changes []*pb.MemberChange) {
	n.changec <- changes
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

	data, err := proto.Marshal(&pb.MemberChangeCol{Changes: changes})
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
		// n.logger.Infof("存储集群成员为raft成员变更日志 %d %d", entry.Index, entry.Term)

		n.raft.raftlog.AppendEntry([]*pb.LogEntry{entry})
		n.raft.cluster.UpdateLogIndex(n.raft.id, entry.Index)
	}
}

func (n *RaftNode) GetLastAppliedIndex() uint64 {
	return n.raft.raftlog.lastAppliedIndex
}

func (n *RaftNode) GetLastLogIndex() uint64 {
	return n.raft.raftlog.lastAppendIndex
}

// 返回发送通道
func (n *RaftNode) SendChan() chan []*pb.RaftMessage {
	return n.sendc
}

// 返回发送通道
func (n *RaftNode) ReadIndexNotifyChan() chan *ReadIndexResp {
	return n.readIndexc
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

func (n *RaftNode) GetElectionTime() int {
	return n.raft.electionTimeout
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
		raft:       NewRaft(id, storage, peers, logger),
		recvc:      make(chan *pb.RaftMessage),
		propc:      make(chan *pb.RaftMessage),
		sendc:      make(chan []*pb.RaftMessage),
		readIndexc: make(chan *ReadIndexResp),
		changec:    make(chan []*pb.MemberChange),
		stopc:      make(chan struct{}),
		ticker:     time.NewTicker(time.Second),
		logger:     logger,
	}

	node.Start()
	return node
}

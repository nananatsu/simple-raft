package server

import (
	"io"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"net"
	"path"
	"strconv"
	"time"

	_ "net/http/pprof"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type RaftServer struct {
	pb.RaftInternalServer

	id     uint64
	bind   string
	peer   map[uint64]*Peer
	node   *raft.RaftNode
	metric chan pb.MessageType
	logger *zap.SugaredLogger
}

func (s *RaftServer) Send(stream pb.RaftInternal_SendServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		p := s.peer[msg.From]

		if p != nil {
			s.metric <- msg.MsgType
			p.Process(msg)
		} else {
			s.logger.Errorf("找不到Raft节点: %d", msg.From)
		}

	}
}

func (s *RaftServer) Put(key, value []byte) {
	s.node.Propose(raft.Encode(key, value))
}

func (s *RaftServer) Ready() bool {
	return s.node.Ready()
}

func (s *RaftServer) IsLeader() bool {
	return s.node.IsLeader()
}

func (s *RaftServer) pollMsg() {

	for {
		msgs := <-s.node.Sendc

		msgMap := make(map[uint64]*pb.RaftMessage, len(s.peer)-1)
		propMap := make(map[uint64]*pb.RaftMessage, len(s.peer)-1)
		otherMsg := make([]*pb.RaftMessage, 0)

		for i, msg := range msgs {
			if s.peer[msg.To] == nil {
				s.logger.Errorf("客户端不存在 %d", msg.To)
				continue
			}
			if msg.MsgType == pb.MessageType_APPEND_ENTRY {
				if msgMap[msg.To] == nil {
					msgMap[msg.To] = msg
				} else {
					entry := msgMap[msg.To].Entry
					if entry[len(entry)-1].Index+1 == msg.Entry[0].Index {
						msgMap[msg.To].LastCommit = msg.LastCommit
						msgMap[msg.To].Entry = append(msgMap[msg.To].Entry, msg.Entry...)
						msgs[i] = nil
					} else if entry[0].Index >= msg.Entry[0].Index {
						msgMap[msg.To] = msg
					}
				}
			} else if msg.MsgType == pb.MessageType_PROPOSE {
				if propMap[msg.To] == nil {
					propMap[msg.To] = msg
				} else {
					propMap[msg.To].Entry = append(propMap[msg.To].Entry, msg.Entry...)
					msgs[i] = nil
				}
			} else {
				otherMsg = append(otherMsg, msg)
			}
		}

		for _, msg := range otherMsg {
			s.metric <- msg.MsgType
			s.peer[msg.To].Send(msg)
		}
		for _, msg := range msgMap {
			s.metric <- msg.MsgType
			s.peer[msg.To].Send(msg)
		}
		for _, msg := range propMap {
			s.metric <- msg.MsgType
			s.peer[msg.To].Send(msg)
		}
	}
}

func (s *RaftServer) showMetrics() {
	appEntryCount := 0
	appEntryResCount := 0
	propCount := 0

	prevAppEntryCount := 0
	prevAppEntryResCount := 0
	prevPropCount := 0
	var prevCommit uint64

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			<-ticker.C
			status := s.node.Status()

			if s.node.IsLeader() {
				s.logger.Infof("发送: %6d, 接收:(响应: %6d, 提议:%6d), 未响应: %6d, 提交: %6d, 已提交: %6d, 未提交： %6d, 接收通道: %6d, 提议通道: %6d, 发送通道: %6d",
					appEntryResCount-prevAppEntryResCount, propCount-prevPropCount, appEntryCount-prevAppEntryCount, appEntryCount-appEntryResCount,
					status.AppliedLogSize-prevCommit, status.AppliedLogSize, status.PendingLogSize,
					status.RecvcSize, status.PropcSize, status.SendcSize)
			} else {
				s.logger.Infof("接收: %6d, 发送:(响应: %6d, 提议:%6d), 未响应: %6d, 提交: %6d, 已提交: %6d, 未提交： %6d, 接收通道: %6d, 提议通道: %6d, 发送通道: %6d",
					appEntryCount-prevAppEntryCount, appEntryResCount-prevAppEntryResCount, propCount-prevPropCount, appEntryCount-appEntryResCount,
					status.AppliedLogSize-prevCommit, status.AppliedLogSize, status.PendingLogSize,
					status.RecvcSize, status.PropcSize, status.SendcSize)
			}

			prevAppEntryCount = appEntryCount
			prevAppEntryResCount = appEntryResCount
			prevPropCount = propCount
			prevCommit = status.AppliedLogSize
		}
	}()

	for {
		t := <-s.metric
		switch t {
		case pb.MessageType_APPEND_ENTRY:
			appEntryCount++
		case pb.MessageType_APPEND_ENTRY_RESP:
			appEntryResCount++
		case pb.MessageType_PROPOSE:
			propCount++
		}
	}

}

func (s *RaftServer) Start() {

	// flag.Parse()
	lis, err := net.Listen("tcp", s.bind)
	if err != nil {
		s.logger.Errorf("启动服务器失败: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	s.logger.Infof("服务器启动成功 %s", s.bind)

	pb.RegisterRaftInternalServer(grpcServer, s)

	go grpcServer.Serve(lis)

	go s.pollMsg()

}

func NewRaftServer(dir string, id uint64, servers map[uint64]string, logger *zap.SugaredLogger) *RaftServer {

	storage := raft.NewRaftStorage(path.Join(dir, "raft_"+strconv.FormatUint(id, 10)), logger)

	var address string
	sids := make([]uint64, 0, len(servers))
	for sid, server := range servers {
		if sid == id {
			address = server
		}
		sids = append(sids, sid)
	}

	node := raft.NewRaftNode(id, storage, sids, logger)
	peers := make(map[uint64]*Peer, len(servers))

	metric := make(chan pb.MessageType, 100000)

	s := &RaftServer{
		logger: logger,
		id:     id,
		bind:   address,
		peer:   peers,
		node:   node,
		metric: metric,
	}

	s.Start()

	go s.showMetrics()

	for sid, server := range servers {
		if sid == id {
			continue
		}
		peers[sid] = NewPeer(sid, server, node, logger)
	}

	return s
}

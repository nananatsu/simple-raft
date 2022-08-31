package server

import (
	"io"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"net"
	"path"
	"strconv"

	_ "net/http/pprof"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type RaftServer struct {
	pb.RaftInternalServer

	id   uint64
	bind string
	peer map[uint64]*Peer

	node   *raft.RaftNode
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
		prevRes := -1
		for i, msg := range msgs {
			if msg.MsgType == pb.MessageType_APPEND_ENTRY {
				if msgMap[msg.To] == nil {
					msgMap[msg.To] = msg
				} else {
					entry := msgMap[msg.To].Entry
					if entry[len(entry)-1].Index+1 != msg.Entry[0].Index {
						continue
					}
					msgMap[msg.To].LastCommit = msg.LastCommit
					msgMap[msg.To].Entry = append(msgMap[msg.To].Entry, msg.Entry...)
					msgs[i] = nil
				}
			} else if msg.MsgType == pb.MessageType_APPEND_ENTRY_RESP {
				if prevRes != -1 {
					msgs[prevRes] = nil
				}
				prevRes = i
			} else if msg.MsgType == pb.MessageType_PROPOSE {
				if propMap[msg.To] == nil {
					propMap[msg.To] = msg
				} else {
					propMap[msg.To].Entry = append(propMap[msg.To].Entry, msg.Entry...)
					msgs[i] = nil
				}
			}
		}

		for _, msg := range msgs {
			if msg == nil {
				continue
			}
			p := s.peer[msg.To]
			if p != nil {
				size := len(msg.Entry)
				if size > 1000 {
					s.logger.Debugf("发送日志到 %d,范围 %d ~ %d ", msg.To, msg.Entry[0].Index, msg.Entry[size-1].Index)
				}
				p.Send(msg)
			} else {
				s.logger.Errorf("客户端不存在 %d", msg.To)
			}
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

func NewRaftServer(dir string, id uint64, peer []*raft.Peer, logger *zap.SugaredLogger) *RaftServer {

	storage := raft.NewRaftStorage(path.Join(dir, "raft_"+strconv.FormatUint(id, 10)), logger)
	node := raft.NewRaftNode(id, storage, peer, logger)
	peers := make(map[uint64]*Peer, len(peer))

	var address string
	for _, p := range peer {
		if p.Id == id {
			address = p.Server
			continue
		}
		peers[p.Id] = NewPeer(p.Id, p.Server, node, logger)
	}

	s := &RaftServer{
		logger: logger,
		id:     id,
		bind:   address,
		peer:   peers,
		node:   node,
	}

	s.Start()

	for _, p := range peer {
		if p.Id == id {
			continue
		}
		peers[p.Id] = NewPeer(p.Id, p.Server, node, logger)
	}

	return s
}

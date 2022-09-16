package server

import (
	"context"
	"fmt"
	"kvdb/pkg/clientpb"
	"kvdb/pkg/raftpb"
	"net"

	"google.golang.org/grpc"
)

func (s *RaftServer) Leader(context.Context, *clientpb.Request) (*clientpb.Response, error) {
	if !s.node.Ready() {
		return &clientpb.Response{Success: false}, fmt.Errorf("集群未就绪")
	}

	return &clientpb.Response{Success: s.node.IsLeader()}, nil
}

func (s *RaftServer) Ready(context.Context, *clientpb.Request) (*clientpb.Response, error) {
	return &clientpb.Response{Success: s.node.Ready()}, nil
}

func (s *RaftServer) Put(ctx context.Context, req *clientpb.PutRequest) (*clientpb.Response, error) {

	if !s.node.Ready() {
		return &clientpb.Response{Success: false}, fmt.Errorf("集群未就绪")
	}

	// s.logger.Debugf("收到消息 %s ,%s", req.Key, req.Value)

	s.put([]byte(req.Key), []byte(req.Value))

	return &clientpb.Response{Success: true}, nil
}

func (s *RaftServer) Change(ctx context.Context, req *clientpb.ConfigRequest) (*clientpb.Response, error) {

	if !s.node.Ready() {
		return &clientpb.Response{Success: false}, fmt.Errorf("集群未就绪")
	}

	var err error
	if req.Type == clientpb.ConfigType_ADD_NODE {
		err = s.changeMember(req.Servers, raftpb.MemberChangeType_ADD_NODE)
	} else {
		err = s.changeMember(req.Servers, raftpb.MemberChangeType_REMOVE_NODE)
	}

	if err != nil {
		return &clientpb.Response{Success: false}, err
	}
	return &clientpb.Response{Success: true}, nil
}

func (s *RaftServer) StartKvServer() {

	lis, err := net.Listen("tcp", s.serverAddress)
	if err != nil {
		s.logger.Errorf("启动外部服务器失败: %v", err)
	}
	var opts []grpc.ServerOption
	s.kvServer = grpc.NewServer(opts...)

	s.logger.Infof("外部服务器启动成功 %s", s.serverAddress)

	clientpb.RegisterKvdbServer(s.kvServer, s)

	err = s.kvServer.Serve(lis)

	if err != nil {
		s.logger.Errorf("外部服务器关闭: %v", err)
	}
}

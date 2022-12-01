package server

import (
	"context"
	"fmt"
	"kvdb/pkg/clientpb"
	"kvdb/pkg/raftpb"
	"kvdb/pkg/sql"
	"kvdb/pkg/utils"
	"net"

	"google.golang.org/grpc"
)

func (s *RaftServer) Register(ctx context.Context, req *clientpb.Auth) (*clientpb.Response, error) {

	var res *clientpb.Response

	if s.node.IsLeader() {
		res = &clientpb.Response{Success: true, ClientId: utils.NextId(s.id)}
	} else {
		res = &clientpb.Response{Success: false}
	}
	return res, nil
}

func (s *RaftServer) Get(ctx context.Context, req *clientpb.ReadonlyQuery) (*clientpb.Response, error) {
	var res *clientpb.Response
	if s.node.IsLeader() {
		res = &clientpb.Response{Success: true}
	} else {
		res = &clientpb.Response{Success: false}
	}

	return res, nil
}

func (s *RaftServer) Put(ctx context.Context, req *clientpb.Request) (*clientpb.Response, error) {
	if !s.node.Ready() {
		return &clientpb.Response{Success: false}, fmt.Errorf("集群未就绪")
	}

	for _, kp := range req.Cmd.Put.Data {
		err := s.put(kp.Key, kp.Value)

		if err != nil {
			return &clientpb.Response{Success: false}, err
		}

	}

	return &clientpb.Response{Success: true}, nil
}

func (s *RaftServer) Delete(ctx context.Context, req *clientpb.Request) (*clientpb.Response, error) {

	if !s.node.Ready() {
		return &clientpb.Response{Success: false}, fmt.Errorf("集群未就绪")
	}

	for _, k := range req.Cmd.Del.Keys {
		err := s.put(k, nil)
		if err != nil {
			return &clientpb.Response{Success: false}, err
		}
	}

	return &clientpb.Response{Success: true}, nil
}

func (s *RaftServer) Config(ctx context.Context, req *clientpb.Request) (*clientpb.Response, error) {
	if !s.node.Ready() {
		return &clientpb.Response{Success: false}, fmt.Errorf("集群未就绪")
	}

	var err error
	if req.Cmd.Conf.Type == clientpb.ConfigType_ADD_NODE {
		err = s.changeMember(req.Cmd.Conf.Servers, raftpb.MemberChangeType_ADD_NODE)
	} else {
		err = s.changeMember(req.Cmd.Conf.Servers, raftpb.MemberChangeType_REMOVE_NODE)
	}

	if err != nil {
		return &clientpb.Response{Success: false}, err
	}
	return &clientpb.Response{Success: true}, nil

}

func (s *RaftServer) ExecSql(ctx context.Context, sqlStr string) ([]*SQLResult, error) {

	stmts, err := sql.ParseSQL(sqlStr)
	if err != nil {
		return nil, err
	}

	rets := make([]*SQLResult, 0, len(stmts))

	for _, st := range stmts {
		switch st.GetStmtType() {
		case sql.CREATE_STMT:
			stmt, _ := st.(*sql.CreateStmt)
			err := s.ExcuteCreate(stmt)
			if err != nil {
				return nil, err
			}
		case sql.INSERT_STMT:
			stmt, _ := st.(*sql.InsertStmt)
			err := s.ExcuteInsert(stmt)
			if err != nil {
				s.logger.Error(err)
				return nil, err
			}
		case sql.SELECT_STMT:
			stmt := st.(*sql.SelectStmt)
			ret, err := s.ExecuteSelect(stmt)
			if err != nil {
				return nil, err
			}

			rets = append(rets, &SQLResult{Type: sql.SELECT_STMT, SelecResult: ret})
		}
	}

	return rets, nil
}

func (s *RaftServer) StartKvServer() {

	lis, err := net.Listen("tcp", s.serverAddress)
	if err != nil {
		s.logger.Errorf("启动客户端服务器失败: %v", err)
	}
	var opts []grpc.ServerOption
	s.kvServer = grpc.NewServer(opts...)

	s.logger.Infof("客户端服务器启动成功 %s", s.serverAddress)

	clientpb.RegisterKvdbServer(s.kvServer, s)

	err = s.kvServer.Serve(lis)

	if err != nil {
		s.logger.Errorf("外部服务器关闭: %v", err)
	}
}

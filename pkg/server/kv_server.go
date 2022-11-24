package server

import (
	"context"
	"encoding/json"
	"fmt"
	"kvdb/pkg/clientpb"
	"kvdb/pkg/raftpb"
	"kvdb/pkg/sql"
	"kvdb/pkg/utils"
	"net"
	"strconv"

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

func (s *RaftServer) ExecSql(ctx context.Context, sqlStr string) error {

	stmts, err := sql.ParseSQL(sqlStr)

	if err != nil {
		return err
	}

	for _, st := range stmts {
		switch st.GetStmtType() {
		case sql.CREATE_STMT:
			stmt := st.(*sql.CreateStmt)
			fmt.Printf(" create table: %s , def: %+v, index: %+v ", stmt.Table, stmt.Def, stmt.Option)

			tableMetaKey := []byte(strconv.Itoa(1) + "_meta_" + stmt.Table)
			meta, err := s.get(tableMetaKey)
			if err != nil {
				return fmt.Errorf("检查表 %s 是否存在失败 %v", stmt.Table, err)
			}

			if meta != nil {
				return fmt.Errorf("表 %s 已存在失败 %v", stmt.Table, err)
			}

			key := []byte(strconv.Itoa(1) + "_max_table_id")
			value, err := s.get(key)
			if err != nil {
				return fmt.Errorf("获取表 %s 编号失败 %v", stmt.Table, err)
			}

			var tableId int
			if value == nil {
				tableId = 2
			} else {
				tableId, err = strconv.Atoi(string(value))
				if err != nil {
					return fmt.Errorf("解析表 %s 编号失败 %v", stmt.Table, err)
				}
			}

			err = s.put(key, []byte(strconv.Itoa(tableId+1)))
			if err != nil {
				return fmt.Errorf("更新表编号失败 %v", err)
			}

			stmt.TableId = tableId
			table, err := json.Marshal(stmt)
			if err != nil {
				return fmt.Errorf("序列化表 %s 元数据 %+v 失败 %v", stmt.Table, stmt, err)
			}

			err = s.put(tableMetaKey, []byte(table))
			if err != nil {
				return fmt.Errorf("写入表 %s 元数据失败 %v", stmt.Table, err)
			}
		case sql.INSERT_STMT:
			stmt := st.(*sql.InsertStmt)
			fmt.Printf(" insert into: %s column: %+v values: %+v ", stmt.Into, stmt.ColumnList, stmt.ValueList)
		case sql.SELECT_STMT:
			stmt := st.(*sql.SelectStmt)
			fmt.Printf(" select: %+v  from: %+v where: %+v order: %+v limit: %+v", stmt.Filed, stmt.From, stmt.Where, stmt.Order, stmt.Limit)
		}
	}

	return nil
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

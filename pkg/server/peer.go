package server

import (
	"context"
	"io"
	pb "kvdb/pkg/raftpb"
	"strconv"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// 流接口，统一grpc 客户端流、服务器流使用
type Stream interface {
	Send(*pb.RaftMessage) error
	Recv() (*pb.RaftMessage, error)
}

// 远端连接信息
type Remote struct {
	address      string
	conn         *grpc.ClientConn
	client       pb.RaftClient
	clientStream pb.Raft_ConsensusClient
	serverStream pb.Raft_ConsensusServer
}

type Peer struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	id     uint64
	stream Stream  // grpc双向流
	remote *Remote // 远端连接信息

	recvc  chan *pb.RaftMessage // 流读取数据发送通道
	metric chan pb.MessageType  // 监控通道
	close  bool                 // 是否准备关闭
	logger *zap.SugaredLogger
}

// 批量发送消息到远端
func (p *Peer) SendBatch(msgs []*pb.RaftMessage) {
	p.wg.Add(1)
	var appEntryMsg *pb.RaftMessage
	var propMsg *pb.RaftMessage
	for _, msg := range msgs {
		if msg.MsgType == pb.MessageType_APPEND_ENTRY {
			if appEntryMsg == nil {
				appEntryMsg = msg
			} else {
				size := len(appEntryMsg.Entry)
				if size == 0 || len(msg.Entry) == 0 || appEntryMsg.Entry[size-1].Index+1 == msg.Entry[0].Index {
					appEntryMsg.LastCommit = msg.LastCommit
					appEntryMsg.Entry = append(appEntryMsg.Entry, msg.Entry...)
				} else if appEntryMsg.Entry[0].Index >= msg.Entry[0].Index {
					appEntryMsg = msg
				}
			}
		} else if msg.MsgType == pb.MessageType_PROPOSE {
			if propMsg == nil {
				propMsg = msg
			} else {
				propMsg.Entry = append(propMsg.Entry, msg.Entry...)
			}
		} else {
			p.send(msg)
		}
	}

	if appEntryMsg != nil {
		p.send(appEntryMsg)
	}

	if propMsg != nil {
		p.send(propMsg)
	}
	p.wg.Done()
}

// 通过流发送消息
func (p *Peer) send(msg *pb.RaftMessage) {
	if msg == nil {
		return
	}

	if p.stream == nil {
		if err := p.Connect(); err != nil {
			return
		}
	}

	if err := p.stream.Send(msg); err != nil {
		p.logger.Errorf("发送消息 %s 到 %s 失败 ,日志数量: %d %v", msg.MsgType.String(), strconv.FormatUint(msg.To, 16), len(msg.Entry), err)
		return
	}
	p.metric <- msg.MsgType
}

// 阻塞流读取数据
func (p *Peer) Recv() {
	// 接收消息
	for {
		msg, err := p.stream.Recv()
		if err == io.EOF {
			p.stream = nil
			p.logger.Errorf("读取 %s 流结束", strconv.FormatUint(p.id, 16))
			return
		}

		if err != nil {
			p.stream = nil
			p.logger.Errorf("读取 %s 流失败： %v", strconv.FormatUint(p.id, 16), err)
			return
		}
		p.recvc <- msg
	}
}

// 停止
func (p *Peer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.wg.Wait()
	p.logger.Infof("关闭节点 %s 处理协程", strconv.FormatUint(p.id, 16))

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
	}

	if p.remote.conn != nil {
		p.remote.conn.Close()
	}
}

// 设置双向流，流在其他节点连入设置
func (p *Peer) SetStream(stream pb.Raft_ConsensusServer) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream == nil {
		p.stream = stream
		p.remote.serverStream = stream

		if p.remote.clientStream != nil {
			p.remote.clientStream.CloseSend()
			p.remote.conn.Close()
			p.remote.clientStream = nil
			p.remote.conn = nil
		}

		return true
	}
	return false
}

// 主动建立连接，可在断开后重连
func (p *Peer) Reconnect() error {

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
		p.remote.clientStream = nil
		p.stream = nil
	}

	stream, err := p.remote.client.Consensus(context.Background())
	// var delay time.Duration
	for err != nil {
		p.logger.Errorf("连接raft服务 %s 失败: %v", p.remote.address, err)
		return err
		// delay++
		// if delay > 5 {
		// 	return fmt.Errorf("超过最大尝试次数10")
		// }
		// select {
		// case <-time.After(time.Second):
		// 	stream, err = p.remote.client.Consensus(context.Background())
		// case <-p.stopc:
		// 	return fmt.Errorf("任务终止")
		// }
	}

	p.logger.Debugf("创建 %s 读写流", strconv.FormatUint(p.id, 16))
	p.stream = stream
	p.remote.clientStream = stream

	go p.Recv()
	return nil
}

// 主动连接远端
func (p *Peer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stream != nil {
		return nil
	}

	if p.remote.conn == nil {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

		conn, err := grpc.Dial(p.remote.address, opts...)
		if err != nil {
			p.logger.Errorf("创建连接 %s 失败: %v", strconv.FormatUint(p.id, 16), err)
			return err
		}
		// p.logger.Debugf("连接到节点 %s ", strconv.FormatUint(p.id, 16))

		p.remote.conn = conn
		p.remote.client = pb.NewRaftClient(conn)
	}

	return p.Reconnect()
}

func NewPeer(id uint64, address string, recvc chan *pb.RaftMessage, metric chan pb.MessageType, logger *zap.SugaredLogger) *Peer {
	p := &Peer{
		id:     id,
		remote: &Remote{address: address},
		recvc:  recvc,
		metric: metric,
		logger: logger,
	}
	return p
}

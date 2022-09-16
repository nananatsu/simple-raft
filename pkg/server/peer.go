package server

import (
	"context"
	"fmt"
	"io"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Stream interface {
	Send(*pb.RaftMessage) error
	Recv() (*pb.RaftMessage, error)
}

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
	node   *raft.RaftNode
	stream Stream
	remote *Remote
	metric chan pb.MessageType
	recvc  chan *pb.RaftMessage
	propc  chan *pb.RaftMessage
	sendc  chan *pb.RaftMessage
	stopc  chan struct{}
	close  bool
	logger *zap.SugaredLogger
}

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
			p.sendc <- msg
		}
	}

	if appEntryMsg != nil {
		p.sendc <- appEntryMsg
	}

	if propMsg != nil {
		p.sendc <- propMsg
	}
	p.wg.Done()
}

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

func (p *Peer) Process(msg *pb.RaftMessage) (err error) {
	defer func() {
		if reason := recover(); reason != nil {
			err = fmt.Errorf("处理消息 %s 失败:%v", msg.String(), reason)
		}
	}()

	p.metric <- msg.MsgType

	if msg.MsgType == pb.MessageType_PROPOSE {
		p.propc <- msg
	} else {
		p.recvc <- msg
	}
	return nil
}

func (p *Peer) Start() {

	// 处理接收到消息
	go func() {
		for {
			select {
			case <-p.stopc:
				return
			case msg := <-p.recvc:
				p.node.HandleMsg(msg)
			}
		}
	}()

	// 处理接收到提议
	go func() {
		for {
			select {
			case <-p.stopc:
				return
			case msg := <-p.propc:
				p.node.HandlePropose(msg)
			}
		}
	}()

	// 发送消息到连接另一端
	go func() {
		for {
			select {
			case <-p.stopc:
				return
			case msg := <-p.sendc:
				p.send(msg)
			}
		}
	}()
}

func (p *Peer) Recv() {
	// 接收消息
	for {
		msg, err := p.stream.Recv()
		if err == io.EOF {
			p.stream = nil
			p.logger.Errorf("读取%s 流结束", strconv.FormatUint(p.id, 16))
			return
		}

		if err != nil {
			p.stream = nil
			p.logger.Errorf("读取%s 流失败： %v", strconv.FormatUint(p.id, 16), err)
			return
		}

		err = p.Process(msg)
		if err != nil {
			p.logger.Errorf("处理消息失败： %v", err)
		}
	}
}

func (p *Peer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.wg.Wait()
	p.logger.Infof("关闭节点 %s 处理协程", strconv.FormatUint(p.id, 16))

	for {
		select {
		case p.stopc <- struct{}{}:
		case <-time.After(time.Second):

			close(p.recvc)
			close(p.propc)
			close(p.sendc)
			close(p.stopc)

			if p.remote.clientStream != nil {
				p.remote.clientStream.CloseSend()
			}

			if p.remote.conn != nil {
				p.remote.conn.Close()
			}
			return
		}
	}
}

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

func (p *Peer) Reconnect() error {

	if p.remote.clientStream != nil {
		p.remote.clientStream.CloseSend()
		p.remote.clientStream = nil
		p.stream = nil
	}

	stream, err := p.remote.client.Consensus(context.Background())
	var delay time.Duration
	for err != nil {
		p.logger.Errorf("连接raft服务 %s失败: %v", p.remote.address, err)

		delay++
		if delay > 5 {
			return fmt.Errorf("超过最大尝试次数10")
		}
		select {
		case <-time.After(time.Second):
			stream, err = p.remote.client.Consensus(context.Background())
		case <-p.stopc:
			return fmt.Errorf("任务终止")
		}
	}

	p.logger.Debugf("创建 %s 读写流", strconv.FormatUint(p.id, 16))
	p.stream = stream
	p.remote.clientStream = stream

	go p.Recv()
	return nil
}

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

func NewPeer(id uint64, address string, node *raft.RaftNode, metric chan pb.MessageType, logger *zap.SugaredLogger) *Peer {
	p := &Peer{
		id:     id,
		remote: &Remote{address: address},
		node:   node,
		metric: metric,
		recvc:  make(chan *pb.RaftMessage),
		propc:  make(chan *pb.RaftMessage),
		sendc:  make(chan *pb.RaftMessage),
		stopc:  make(chan struct{}),
		logger: logger,
	}
	return p
}

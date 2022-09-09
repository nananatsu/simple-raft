package server

import (
	"context"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	mu         sync.Mutex
	id         uint64
	address    string
	node       *raft.RaftNode
	close      bool
	connecting bool
	client     pb.RaftInternalClient
	sendClient pb.RaftInternal_SendClient
	recvc      chan *pb.RaftMessage
	propc      chan *pb.RaftMessage
	sendc      chan *pb.RaftMessage
	stopc      chan struct{}
	logger     *zap.SugaredLogger
}

func (p *Peer) Send(msg *pb.RaftMessage) {
	p.sendc <- msg
}

func (p *Peer) Process(msg *pb.RaftMessage) {
	if msg.MsgType == pb.MessageType_PROPOSE {
		p.propc <- msg
	} else {
		p.recvc <- msg
	}
}

func (p *Peer) Start() {
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

	go func() {
		for {
			select {
			case <-p.stopc:
				return
			case msg := <-p.sendc:
				if msg == nil {
					continue
				}
				if p.sendClient == nil {
					p.Connect()
				}

				if err := p.sendClient.Send(msg); err != nil {
					p.logger.Errorf("发送消息 %s 失败 ,日志数量: %d %v", msg.MsgType.String(), len(msg.Entry), err)

					if !p.connecting {
						p.Reconnect()
					}
				}
			}
		}
	}()

}

func (p *Peer) Stop() {

	p.logger.Infof("关闭节点 %s连接", strconv.FormatUint(p.id, 16))
	p.stopc <- struct{}{}
	close(p.recvc)
	close(p.propc)
	close(p.sendc)
	close(p.stopc)
	if p.sendClient != nil {
		p.sendClient.CloseSend()
	}
}

func (p *Peer) Reconnect() (err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connecting = true

	if p.sendClient != nil {
		p.sendClient.CloseSend()
		p.sendClient = nil
	}

	p.sendClient, err = p.client.Send(context.Background())

	var count time.Duration
	for err != nil {
		p.logger.Errorf("连接raft服务 %s失败: %v", p.address, err)

		count++
		select {
		case <-time.After(count * time.Second):
			p.sendClient, err = p.client.Send(context.Background())
		case <-p.stopc:
			return
		}
	}
	p.connecting = false
	return
}

func (p *Peer) Connect() error {

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.Dial(p.address, opts...)
	if err != nil {
		p.logger.Errorln("创建客户端连接失败", err)
		return err
	}

	p.client = pb.NewRaftInternalClient(conn)
	p.Reconnect()

	return nil
}

func NewPeer(id uint64, address string, node *raft.RaftNode, logger *zap.SugaredLogger) *Peer {
	p := &Peer{
		id:      id,
		address: address,
		node:    node,
		recvc:   make(chan *pb.RaftMessage),
		propc:   make(chan *pb.RaftMessage),
		sendc:   make(chan *pb.RaftMessage),
		stopc:   make(chan struct{}),
		logger:  logger,
	}
	p.Start()

	return p
}

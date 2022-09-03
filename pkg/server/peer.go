package server

import (
	"context"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Peer struct {
	mu sync.Mutex

	id      uint64
	address string

	node *raft.RaftNode

	connecting bool
	client     pb.RaftInternalClient
	sendClient pb.RaftInternal_SendClient

	Recvc     chan *pb.RaftMessage
	Propc     chan *pb.RaftMessage
	Sendc     chan *pb.RaftMessage
	PropSendc chan *pb.RaftMessage

	logger *zap.SugaredLogger
}

func (p *Peer) Send(msg *pb.RaftMessage) {

	if msg.MsgType == pb.MessageType_PROPOSE {
		p.PropSendc <- msg
	} else {
		p.Sendc <- msg
	}
}

func (p *Peer) Process(msg *pb.RaftMessage) {
	if msg.MsgType == pb.MessageType_PROPOSE {
		p.Propc <- msg
	} else {
		p.Recvc <- msg
	}
}

func (p *Peer) Start() {
	// go p.Connect()

	go func() {
		for {
			msg := <-p.Recvc
			p.node.HandleMsg(msg)
		}
	}()

	go func() {
		for {
			msg := <-p.Propc
			p.node.HandlePropose(msg)
		}
	}()

	go func() {
		var propSendc chan *pb.RaftMessage
		for {
			var msg *pb.RaftMessage
			if len(p.Sendc) > 0 {
				propSendc = nil
			} else {
				propSendc = p.PropSendc
			}

			select {
			case msg = <-p.Sendc:
			case msg = <-propSendc:
			}

			if p.sendClient == nil {
				p.Connect()
			}

			if err := p.sendClient.Send(msg); err != nil {
				p.logger.Errorf("发送消息失败,消息类型: %s ,日志数量: %d %v", msg.MsgType.Descriptor().FullName(), len(msg.Entry), err)

				if !p.connecting {
					p.Reconnect()
				}
			}
		}
	}()

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
		time.Sleep(count * time.Second)
		p.sendClient, err = p.client.Send(context.Background())
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

	propc := make(chan *pb.RaftMessage, 1000)
	recvc := make(chan *pb.RaftMessage, 1000)
	sendc := make(chan *pb.RaftMessage, 1000)
	propSendc := make(chan *pb.RaftMessage, 1000)

	p := &Peer{
		id:        id,
		address:   address,
		node:      node,
		Recvc:     recvc,
		Propc:     propc,
		Sendc:     sendc,
		PropSendc: propSendc,
		logger:    logger,
	}
	p.Start()

	return p
}

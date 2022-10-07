package server

import (
	"crypto/sha1"
	"encoding/binary"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"path"
	"strconv"

	"go.uber.org/zap"
)

type Config struct {
	Dir           string
	Name          string
	PeerAddress   string
	ServerAddress string
	Peers         map[string]string
	Logger        *zap.SugaredLogger
}

func GenerateNodeId(name string) uint64 {
	hash := sha1.Sum([]byte(name))
	return binary.BigEndian.Uint64(hash[:8])
}

func Bootstrap(conf *Config) *RaftServer {

	dir := path.Join(conf.Dir, conf.Name)
	storage := raft.NewRaftStorage(dir, &raft.SimpleEncoding{}, conf.Logger)
	peers, err := storage.RestoreMember()

	if err != nil {
		conf.Logger.Errorf("恢复集群成员失败: %v", err)
	}

	var nodeId uint64
	var node *raft.RaftNode
	metric := make(chan pb.MessageType, 1000)
	servers := make(map[uint64]*Peer, len(conf.Peers))

	if len(peers) != 0 {
		nodeId = GenerateNodeId(conf.Name)
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
	} else {
		peers = make(map[uint64]string, len(conf.Peers))
		// 遍历节点配置，生成各节点id
		for name, address := range conf.Peers {
			id := GenerateNodeId(name)
			peers[id] = address
			if name == conf.Name {
				nodeId = id
			}
		}
		node = raft.NewRaftNode(nodeId, storage, peers, conf.Logger)
		node.InitMember(peers)
	}

	// 初始化远端节点配置
	for id, address := range peers {
		conf.Logger.Infof("集群成员 %s 地址 %s", strconv.FormatUint(id, 16), address)
		if id == nodeId {
			continue
		}
		servers[id] = NewPeer(id, address, node, metric, conf.Logger)
	}

	server := &RaftServer{
		logger:        conf.Logger,
		dir:           dir,
		id:            nodeId,
		name:          conf.Name,
		peerAddress:   conf.PeerAddress,
		serverAddress: conf.ServerAddress,
		peers:         servers,
		encoding:      &raft.SimpleEncoding{},
		node:          node,
		storage:       storage,
		metric:        metric,
		stopc:         make(chan struct{}),
	}

	return server
}

package server

import (
	"crypto/sha1"
	"encoding/binary"
	"kvdb/pkg/raft"
	pb "kvdb/pkg/raftpb"
	"path"

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

	var nodeId uint64
	metric := make(chan pb.MessageType, 1000)

	peers := make(map[uint64]string, len(conf.Peers))
	nodeIds := make([]uint64, 0, len(conf.Peers))
	servers := make(map[uint64]*Peer, len(conf.Peers))

	// 遍历节点配置，生成各节点id
	for name, address := range conf.Peers {
		id := GenerateNodeId(name)
		peers[id] = address
		nodeIds = append(nodeIds, id)
		if name == conf.Name {
			nodeId = id
		}
	}

	dir := path.Join(conf.Dir, conf.Name)
	conf.Logger.Debugf("工作目录: %s", dir)

	// 初始化raft节点
	storage := raft.NewRaftStorage(dir, conf.Logger)
	node := raft.NewRaftNode(nodeId, storage, nodeIds, conf.Logger)

	// 初始化远端节点配置
	for id, address := range peers {
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
		node:          node,
		metric:        metric,
		stopc:         make(chan struct{}),
	}

	return server
}

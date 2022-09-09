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
	Dir     string
	Name    string
	Address string
	Peers   map[string]string
	Logger  *zap.SugaredLogger
}

func GenerateNodeId(name string) uint64 {
	hash := sha1.Sum([]byte(name))
	return binary.BigEndian.Uint64(hash[:8])
}

func Bootstrap(conf *Config) *RaftServer {

	var nodeId uint64
	peers := make(map[uint64]string, len(conf.Peers))
	nodeIds := make([]uint64, 0, len(conf.Peers))

	for name, address := range conf.Peers {
		id := GenerateNodeId(name)
		peers[id] = address
		nodeIds = append(nodeIds, id)
		if name == conf.Name {
			nodeId = id
		}
	}

	dir := path.Join(conf.Dir, conf.Name)

	storage := raft.NewRaftStorage(dir, conf.Logger)
	node := raft.NewRaftNode(nodeId, storage, nodeIds, conf.Logger)
	metric := make(chan pb.MessageType, 1000)

	server := &RaftServer{
		logger: conf.Logger,
		dir:    dir,
		id:     nodeId,
		name:   conf.Name,
		bind:   conf.Address,
		peer:   make(map[uint64]*Peer, len(conf.Peers)),
		node:   node,
		stroge: storage,
		metric: metric,
	}

	server.Start()
	server.showMetrics()

	for id, address := range peers {
		if id == nodeId {
			continue
		}
		server.peer[id] = NewPeer(id, address, node, conf.Logger)
	}

	return server
}

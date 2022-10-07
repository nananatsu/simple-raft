package server

import (
	"encoding/binary"
	"fmt"
	"kvdb/pkg/raftpb"
	"kvdb/pkg/utils"
	"math/rand"
	"testing"
	"time"
)

func InitServer(clusterNumber int) (map[string]string, []*RaftServer) {
	peers := make(map[string]string, clusterNumber)
	servers := make([]*Config, clusterNumber)

	dir := "../../build/"
	for i := 0; i < clusterNumber; i++ {
		name := fmt.Sprintf("raft_%d", i+1)
		logger := utils.GetLogger(dir + name)
		sugar := logger.Sugar()

		peerAddress := fmt.Sprintf("localhost:%d", 9123+i)
		peers[name] = peerAddress

		servers[i] = &Config{
			Dir:           dir,
			Name:          name,
			PeerAddress:   peerAddress,
			ServerAddress: fmt.Sprintf("localhost:%d", 9223+i),
			Peers:         peers,
			Logger:        sugar,
		}
	}

	rafts := make([]*RaftServer, clusterNumber)
	for i, conf := range servers {
		s := Bootstrap(conf)
		go s.Start()
		rafts[i] = s
	}

	return peers, rafts
}

func TestClusterVote(t *testing.T) {

	InitServer(3)

	<-time.After(600 * time.Second)
}

func TestClusterPropose(t *testing.T) {

	_, servers := InitServer(3)

	for _, s := range servers {
		go func(s *RaftServer) {
			for {
				time.Sleep(3 * time.Second)
				if s.node.Ready() && s.node.IsLeader() {
					for i := 0; i < 10000000; i++ {
						key := utils.RandStringBytesRmndr(rand.Intn(10) + 10)
						value := utils.RandStringBytesRmndr(20)
						s.put(key, value)
					}
					break
				}
			}
		}(s)
	}

	<-time.After(600 * time.Second)
}

func TestClusterMemberChange(t *testing.T) {

	dir := "../../build/"
	peers, servers := InitServer(3)
	for _, s := range servers {
		go func(s *RaftServer) {
			for {
				time.Sleep(3 * time.Second)
				if s.node.Ready() && s.node.IsLeader() {

					name := "raft_4"
					peerAddress := fmt.Sprintf("localhost:%d", 9123+3)
					logger := utils.GetLogger(dir + name)
					sugar := logger.Sugar()

					newConf := &Config{
						Dir:           dir,
						Name:          name,
						PeerAddress:   peerAddress,
						ServerAddress: fmt.Sprintf("localhost:%d", 9223+3),
						Peers:         peers,
						Logger:        sugar,
					}

					nodes := make(map[string]string)
					nodes[name] = peerAddress
					peers[name] = peerAddress

					s.changeMember(nodes, raftpb.MemberChangeType_ADD_NODE)

					sn := Bootstrap(newConf)
					go sn.Start()
					break

					// for {
					// 	time.Sleep(5 * time.Second)
					// 	if s.node.Ready() {
					// 		s.changeMember(nodes, raftpb.MemberChangeType_REMOVE_NODE)
					// 		break
					// 	}
					// }

					// for i := 0; i < 10000; i++ {
					// 	key := utils.RandStringBytesRmndr(rand.Intn(10) + 10)
					// 	value := utils.RandStringBytesRmndr(20)
					// 	s.put(key, value)
					// }
				}
			}
		}(s)
	}

	<-time.After(600 * time.Second)

}

func TestReadIndex(t *testing.T) {

	_, servers := InitServer(3)
	for _, s := range servers {
		go func(s *RaftServer) {
			for {
				time.Sleep(3 * time.Second)
				if s.node.Ready() && s.node.IsLeader() {

					b := make([]byte, 8)
					reqId := utils.NextId(s.id)
					binary.BigEndian.PutUint64(b, reqId)

					idx, err := s.readIndex(b)

					s.logger.Debugf("节点最新提交 %d %v", idx, err)
				}
			}
		}(s)
	}

	<-time.After(600 * time.Second)
}

func TestGet(t *testing.T) {

	_, servers := InitServer(3)
	for _, s := range servers {
		go func(s *RaftServer) {
			for {
				time.Sleep(3 * time.Second)
				if s.node.Ready() && s.node.IsLeader() {

					k := []byte("hello")
					v := []byte("world")

					err := s.put(k, v)

					if err != nil {
						s.logger.Debugf("写入失败  %v", err)
					}

					for {
						ret, err := s.get(k)

						s.logger.Debugf("查询结果 %s %v", string(ret), err)

						if ret != nil {
							break
						}
						time.Sleep(3 * time.Second)
					}
					break
				}
			}
		}(s)
	}

	<-time.After(600 * time.Second)
}

package server

import (
	"fmt"
	"kvdb/pkg/raftpb"
	"kvdb/pkg/utils"
	"math/rand"
	"testing"
	"time"
)

func InitServer() (map[string]string, []*RaftServer) {

	clusterNumber := 3
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

	InitServer()

	<-time.After(60 * time.Second)
}

func TestClusterPropose(t *testing.T) {

	_, servers := InitServer()

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
	peers, servers := InitServer()
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

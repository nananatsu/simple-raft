package client

import (
	"context"
	"fmt"
	pb "kvdb/pkg/clientpb"
	"kvdb/pkg/server"
	"kvdb/pkg/utils"
	"testing"
	"time"
)

func InitServer() ([]string, []*server.RaftServer, func(string, string, string)) {

	clusterNumber := 3

	connects := make([]string, clusterNumber)
	peers := make(map[string]string, clusterNumber)
	rafts := make([]*server.RaftServer, clusterNumber)
	servers := make([]*server.Config, clusterNumber)

	dir := "../../build/"
	for i := 0; i < clusterNumber; i++ {
		name := fmt.Sprintf("raft_%d", i+1)
		logger := utils.GetLogger(dir + name)
		sugar := logger.Sugar()

		peerAddress := fmt.Sprintf("localhost:%d", 9123+i)
		serverAddress := fmt.Sprintf("localhost:%d", 9223+i)
		peers[name] = peerAddress
		connects[i] = serverAddress

		servers[i] = &server.Config{
			Dir:           dir,
			Name:          name,
			PeerAddress:   peerAddress,
			ServerAddress: serverAddress,
			Peers:         peers,
			Logger:        sugar,
		}
	}

	for i, conf := range servers {
		s := server.Bootstrap(conf)
		go s.Start()
		rafts[i] = s
	}

	addServer := func(name, peerAddress, serverAddress string) {

		logger := utils.GetLogger(dir + name)
		sugar := logger.Sugar()
		peers[name] = peerAddress
		connects = append(connects, serverAddress)

		conf := &server.Config{
			Dir:           dir,
			Name:          name,
			PeerAddress:   peerAddress,
			ServerAddress: serverAddress,
			Peers:         peers,
			Logger:        sugar,
		}

		s := server.Bootstrap(conf)
		go s.Start()
	}

	return connects, rafts, addServer
}

func TestPut(t *testing.T) {

	servers, _, _ := InitServer()

	logger := utils.GetLogger("../../build/")
	sugar := logger.Sugar()

	client := NewClient(servers, sugar)
	client.Connect()
	client.Put("heello", "world")

}

func TestAddNode(t *testing.T) {

	servers, _, addServer := InitServer()

	logger := utils.GetLogger("../../build/")
	sugar := logger.Sugar()

	client := NewClient(servers, sugar)
	client.Connect()

	name := "raft_4"
	peerAddress := fmt.Sprintf("localhost:%d", 9123+3)
	serverAddress := fmt.Sprintf("localhost:%d", 9223+3)

	nodes := make(map[string]string)
	nodes[name] = peerAddress

	client.AddNode(nodes)
	addServer(name, peerAddress, serverAddress)

	<-time.After(60 * time.Second)

}

func TestRemoveNode(t *testing.T) {

	servers, _, addServer := InitServer()

	logger := utils.GetLogger("../../build/")
	sugar := logger.Sugar()

	client := NewClient(servers, sugar)
	client.Connect()

	name := "raft_4"
	peerAddress := fmt.Sprintf("localhost:%d", 9123+3)
	serverAddress := fmt.Sprintf("localhost:%d", 9223+3)

	nodes := make(map[string]string)
	nodes[name] = peerAddress

	client.AddNode(nodes)
	addServer(name, peerAddress, serverAddress)

	resp, _ := client.client.Ready(context.Background(), &pb.Request{})
	for !resp.Success {
		time.Sleep(10 * time.Second)
		resp, _ = client.client.Ready(context.Background(), &pb.Request{})
	}

	client.RemoveNode(nodes)
	<-time.After(300 * time.Second)

}

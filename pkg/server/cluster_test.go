package server

import (
	"fmt"
	"kvdb/pkg/utils"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestClusterVote(t *testing.T) {

	serverMap := make(map[string]string)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("raft_%d", i+1)
		serverMap[name] = fmt.Sprintf("localhost:%d", 9123+i)
	}

	for name, address := range serverMap {
		logger := utils.GetLogger("../../build/" + name)
		sugar := logger.Sugar()

		go func(name string, address string) {
			Bootstrap(&Config{
				Dir:     "../../build/",
				Name:    name,
				Address: address,
				Peers:   serverMap,
				Logger:  sugar,
			})
		}(name, address)
	}

	<-time.After(60 * time.Second)

}

func TestClusterPropose(t *testing.T) {

	serverMap := make(map[string]string)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("raft_%d", i+1)
		serverMap[name] = fmt.Sprintf("localhost:%d", 9123+i)
	}

	metricChan := make(chan int, 1000)

	for name, address := range serverMap {
		logger := utils.GetLogger("../../build/" + name)
		sugar := logger.Sugar()

		go func(name string, address string) {
			s := Bootstrap(&Config{
				Dir:     "../../build/",
				Name:    name,
				Address: address,
				Peers:   serverMap,
				Logger:  sugar,
			})

			go func(s *RaftServer) {
				for {
					time.Sleep(3 * time.Second)
					if s.Ready() {
						if s.IsLeader() {
							for i := 0; i < 10000000; i++ {
								key := utils.RandStringBytesRmndr(rand.Intn(10) + 10)
								value := utils.RandStringBytesRmndr(20)
								s.Put(key, value)
								metricChan <- 1
							}
						}
						break
					}
				}
			}(s)

		}(name, address)
	}

	ticker := time.NewTicker(60 * time.Second)
	n := 0
	for {
		select {
		case <-metricChan:
			n++
		case <-ticker.C:
			if n > 0 {
				log.Println("处理数据：", n)
			}
			n = 0
		}
	}

}

func TestClusterMemberChange(t *testing.T) {

	serverMap := make(map[string]string)
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("raft_%d", i+1)
		serverMap[name] = fmt.Sprintf("localhost:%d", 9123+i)
	}

	for k, server := range serverMap {

		logger := utils.GetLogger("../../build/" + k)
		sugar := logger.Sugar()

		s := Bootstrap(&Config{
			Dir:     "../../build/",
			Name:    k,
			Address: server,
			Peers:   serverMap,
			Logger:  sugar,
		})

		go func(s *RaftServer) {
			for {
				time.Sleep(3 * time.Second)
				if s.Ready() {
					if s.IsLeader() {

						name := "raft_4"
						address := fmt.Sprintf("localhost:%d", 9223)
						logger := utils.GetLogger("../../build/" + name)
						sugar := logger.Sugar()

						nodes := make(map[string]string)
						nodes[name] = address
						serverMap[name] = address
						s.AddNode(nodes)

						Bootstrap(&Config{
							Dir:     "../../build/",
							Name:    name,
							Address: address,
							Peers:   serverMap,
							Logger:  sugar,
						})

						for {
							time.Sleep(5 * time.Second)
							if s.Ready() {
								s.RemoveNode(nodes)
								break
							}
						}
					}
					break
				}
			}
		}(s)

	}

	<-time.After(60 * time.Second)

}

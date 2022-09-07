package server

import (
	"fmt"
	"kvdb/pkg/utils"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestServerVote(t *testing.T) {

	serverMap := make(map[uint64]string)
	for i := 0; i < 3; i++ {
		serverMap[uint64(i+1)] = fmt.Sprintf("localhost:%d", 9123+i)
	}

	metricChan := make(chan int, 10000)
	for i := range serverMap {
		name := fmt.Sprintf("raft_%d", i)
		logger := utils.GetLogger("../../build/" + name)
		sugar := logger.Sugar()
		go func(idx uint64) {
			NewRaftServer("../../build/", idx, serverMap, sugar)
		}(i)
	}

	ticker := time.NewTicker(1 * time.Second)
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

func TestServerLog(t *testing.T) {

	serverMap := make(map[uint64]string)
	for i := 0; i < 3; i++ {
		serverMap[uint64(i+1)] = fmt.Sprintf("localhost:%d", 9123+i)
	}

	metricChan := make(chan int, 10000)
	for i := range serverMap {
		name := fmt.Sprintf("raft_%d", i)
		logger := utils.GetLogger("../../build/" + name)
		sugar := logger.Sugar()
		go func(idx uint64) {
			s := NewRaftServer("../../build/", idx, serverMap, sugar)

			go func(s *RaftServer) {
				for {
					time.Sleep(3 * time.Second)
					if s.Ready() {
						if s.IsLeader() {
							for i := 0; i < 10000000; i++ {
								key := utils.RandStringBytesRmndr(rand.Intn(10) + 10)
								value := utils.RandStringBytesRmndr(20)
								s.Put(key, value)
								// metricChan <- 1
							}
						}
						break
					}
				}
			}(s)
		}(i)
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

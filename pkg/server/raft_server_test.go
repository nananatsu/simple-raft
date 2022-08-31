package server

import (
	"fmt"
	"kvdb/pkg/raft"
	"kvdb/pkg/utils"
	"log"
	"strconv"
	"testing"
	"time"
)

func TestServer(t *testing.T) {

	peers := make([]*raft.Peer, 3)
	for i := 0; i < 3; i++ {
		peers[i] = &raft.Peer{
			Id:     uint64(i + 1),
			Server: fmt.Sprintf("localhost:%d", 9123+i),
		}
	}

	metricChan := make(chan int, 10000)
	for i := 0; i < 3; i++ {
		id := "raft_" + strconv.Itoa(i+1)
		logger := utils.GetLogger("../../build/" + id)
		sugar := logger.Sugar()
		go func(idx int) {
			s := NewRaftServer("../../build/", uint64(idx+1), peers, sugar)
			if idx != -1 {
				go func(s *RaftServer) {
					for {
						time.Sleep(3 * time.Second)
						if s.Ready() {
							// if s.IsLeader() {
							for i := 0; i < 100000; i++ {
								key := utils.RandStringBytesRmndr(8)
								value := utils.RandStringBytesRmndr(10)
								s.Put(key, value)
								metricChan <- 1
							}
							// }
							break
						}
					}
				}(s)
			}
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
				log.Println("处理数据条数：", n)
			}
			n = 0
		}
	}

}

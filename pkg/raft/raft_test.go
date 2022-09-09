package raft

import (
	pb "kvdb/pkg/raftpb"
	"kvdb/pkg/utils"
	"log"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewRaft(t *testing.T) {

	peers := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		peers[i] = uint64(i + 1)
	}
	for i := 0; i < 3; i++ {
		id := "raft_" + strconv.Itoa(i)
		logger := utils.GetLogger("../../build/" + id)
		sugar := logger.Sugar()
		storage := NewRaftStorage("../../build/raft_"+strconv.Itoa(i), sugar)
		go func(idx int) {
			NewRaft(uint64(idx+1), storage, peers, sugar)
		}(i)
	}

	<-time.After(600 * time.Second)

}

func TestNewRaftPropose(t *testing.T) {

	peers := make([]uint64, 3)
	for i := 0; i < 3; i++ {
		peers[i] = uint64(i + 1)
	}

	metricChan := make(chan int, 10000)
	for i := 0; i < 3; i++ {

		id := "raft_" + strconv.Itoa(i)

		logger := utils.GetLogger("../../build/" + id)
		sugar := logger.Sugar()
		storage := NewRaftStorage("../../build/raft_"+strconv.Itoa(i), sugar)

		go func(idx int, logger *zap.SugaredLogger) {
			node := NewRaftNode(uint64(idx+1), storage, peers, logger)
			go func(node *RaftNode) {
				for {
					time.Sleep(3 * time.Second)
					if node.Ready() {
						for i := 0; i < 1000000; i++ {
							key := utils.RandStringBytesRmndr(8)
							value := utils.RandStringBytesRmndr(10)
							node.Propose([]*pb.LogEntry{{Data: Encode(key, value)}})
							metricChan <- 1
						}
						break
					}
				}
			}(node)
		}(i, sugar)
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

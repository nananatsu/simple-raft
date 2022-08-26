package raft

import (
	"kvdb/pkg/utils"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewRaft(t *testing.T) {
	peers := make([]*Peer, 3)
	for i := 0; i < 3; i++ {
		peers[i] = &Peer{
			Id:    uint64(i + 1),
			Recvc: make(chan Message, 1000),
		}
	}
	for i := 0; i < 3; i++ {
		storage := NewRaftStorage("../../build/raft_" + strconv.Itoa(i))
		go func(idx int) {
			NewRaft(uint64(idx+1), peers[idx].Recvc, storage, peers, nil)
		}(i)
	}

	<-time.After(600 * time.Second)

}

func TestNewRaftPropose(t *testing.T) {
	var node *Raft

	peers := make([]*Peer, 3)
	for i := 0; i < 3; i++ {
		peers[i] = &Peer{
			Id:    uint64(i + 1),
			Recvc: make(chan Message, 1000),
		}
	}
	for i := 0; i < 3; i++ {

		id := "raft_" + strconv.Itoa(i)

		storage := NewRaftStorage("../../build/raft_" + strconv.Itoa(i))
		logger := utils.GetLogger("../../build/" + id)
		sugar := logger.Sugar()

		go func(idx int, logger *zap.SugaredLogger) {
			node = NewRaft(uint64(idx+1), peers[idx].Recvc, storage, peers, logger)
		}(i, sugar)
	}

	for {
		time.Sleep(time.Second)
		if node.state == LEADER_STATE || node.leader != 0 {
			for i := 0; i < 100; i++ {
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
				node.Propose([]byte(utils.RandStringBytesRmndr(10)))
			}
			break
		}
	}

	<-time.After(600 * time.Second)

}

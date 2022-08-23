package raft

import (
	"kvdb/pkg/table"
	"strconv"
	"testing"
	"time"
)

func TestNewRaft(t *testing.T) {

	peers := make([]*Peer, 3)
	for i := 0; i < 3; i++ {
		peers[i] = &Peer{
			Id:    int64(i),
			Recvc: make(chan Message, 1000),
		}
	}
	for i := 0; i < 3; i++ {
		storage := table.NewTable("../../build/raft_" + strconv.Itoa(i))

		go NewRaft(int64(i), peers[i].Recvc, storage, peers)
	}

	<-time.After(600 * time.Second)

}

package raft

import (
	"encoding/binary"
	"log"
	"strconv"
	"testing"
)

func TestStorage(t *testing.T) {

	for i := 0; i < 1; i++ {
		storage := NewRaftStorage("../../build/raft_" + strconv.Itoa(i))

		minI, _ := binary.Uvarint(storage.GetFirstIndex())

		maxI, _ := binary.Uvarint(storage.GetLastIndex())
		log.Println(minI, maxI)
	}

}

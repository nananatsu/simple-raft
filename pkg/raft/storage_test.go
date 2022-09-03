package raft

import (
	"strconv"
	"testing"

	"go.uber.org/zap"
)

func TestStorage(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	for i := 0; i < 1; i++ {

		storage := NewRaftStorage("../../build/raft_"+strconv.Itoa(i), sugar)

		// minIndex, _ := storage.GetFirst()
		maxIndex, _ := storage.GetLast()

		sugar.Infoln(maxIndex)
	}

}

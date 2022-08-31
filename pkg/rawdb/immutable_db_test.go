package rawdb

import (
	"kvdb/pkg/utils"
	"math/rand"
	"testing"

	"go.uber.org/zap"
)

func TestImmutableDBFlush(t *testing.T) {

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	mt := NewMemDB(devTestPath, 0, sugar)
	for i := 0; i < 100000; i++ {
		mt.Put([]byte(utils.RandStringBytesRmndr(6)), []byte(utils.RandStringBytesRmndr(rand.Intn(10)+10)))
	}

	table := NewImmutableMemDB(devTestPath, mt)
	table.Flush()
}

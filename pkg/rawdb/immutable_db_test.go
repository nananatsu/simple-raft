package rawdb

import (
	"kvdb/pkg/utils"
	"math/rand"
	"testing"
)

func TestImmutableDBFlush(t *testing.T) {

	mt := NewMemDB(devTestPath, 0)
	for i := 0; i < 100000; i++ {
		mt.Put([]byte(utils.RandStringBytesRmndr(6)), []byte(utils.RandStringBytesRmndr(rand.Intn(10)+10)))
	}

	table := NewImmutableMemDB(devTestPath, mt)
	table.Flush()
}

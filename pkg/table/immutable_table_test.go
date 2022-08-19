package table

import (
	"kvdb/pkg/utils"
	"math/rand"
	"testing"
)

func TestImmutableTableFlush(t *testing.T) {

	mt := NewMemTable(devTestPath, 0)
	for i := 0; i < 100000; i++ {
		mt.Put([]byte(utils.RandStringBytesRmndr(6)), []byte(utils.RandStringBytesRmndr(rand.Intn(10)+10)))
	}

	table := NewImmutableMemTable(devTestPath, mt)
	table.FlushMemTable()
}

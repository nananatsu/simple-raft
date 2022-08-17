package table

import (
	"kvdb/pkg/memtable/skiplist"
	"kvdb/pkg/utils"
	"log"
	"math/rand"
	"os"
	"path"
	"testing"
)

func TestImmutableTableFlush(t *testing.T) {

	pwd, err := os.Getwd()
	if err != nil {
		t.Error("获取当前目录失败")
	}

	sl := skiplist.NewMemTable()

	for i := 0; i < 100000; i++ {
		sl.Put([]byte(utils.RandStringBytesRmndr(6)), []byte(utils.RandStringBytesRmndr(rand.Intn(10)+10)))
	}

	fd, err := os.OpenFile(path.Join(pwd, "sst", "0.0.sst"), os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		log.Println("创建sst文件失败", err)
	}

	table := NewImmutableMemTable(fd, sl)
	table.FlushMemTable()
}

package table

import (
	"kvdb/pkg/utils"
	"math/rand"
	"os"
	"path"
	"testing"
)

func TestNewTable(t *testing.T) {

	pwd, err := os.Getwd()
	if err != nil {
		t.Error("获取当前目录失败")
	}

	table := NewTable(path.Join(pwd, "sst"))

	for i := 0; i < 500000; i++ {
		table.Put(utils.RandStringBytesRmndr(8), utils.RandStringBytesRmndr(rand.Intn(10)+10))
	}

}

func TestReadTable(t *testing.T) {

	pwd, err := os.Getwd()
	if err != nil {
		t.Error("获取当前目录失败")
	}

	readSstTable(path.Join(pwd, "sst/0.0.sst"))

}

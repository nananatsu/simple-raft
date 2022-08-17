package table

import (
	"kvdb/pkg/utils"
	"log"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

func TestNewTable(t *testing.T) {

	pwd, err := os.Getwd()
	if err != nil {
		t.Error("获取当前目录失败")
	}

	NewTable(path.Join(pwd, "sst"))

	timer := time.NewTimer(600 * time.Second)

	<-timer.C
}

func TestTableFlush(t *testing.T) {

	pwd, err := os.Getwd()
	if err != nil {
		t.Error("获取当前目录失败")
	}

	metricChan := make(chan int, 10000)

	table := NewTable(path.Join(pwd, "sst"))

	for i := 0; i < 10; i++ {
		go func(chan int) {
			for {
				table.Put(utils.RandStringBytesRmndr(12), utils.RandStringBytesRmndr(rand.Intn(10)+10))
				metricChan <- 1
			}
		}(metricChan)
	}

	ticker := time.NewTicker(1 * time.Second)
	n := 0
	for {
		select {
		case <-metricChan:
			n++
		case <-ticker.C:
			log.Println("处理数据条数：", n)
			n = 0
		}
	}
}

func TestCompactionTable(t *testing.T) {

	pwd, err := os.Getwd()
	if err != nil {
		t.Error("获取当前目录失败")
	}

	table := NewTable(path.Join(pwd, "sst"))

	table.levelTree.Compaction(0)

}

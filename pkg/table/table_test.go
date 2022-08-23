package table

import (
	"kvdb/pkg/utils"
	"log"
	"math/rand"
	"testing"
	"time"
)

const devTestPath = "../../build/sst"

func TestNewTable(t *testing.T) {

	NewTable(devTestPath)

	<-time.After(600 * time.Second)
}

func TestTableGet(t *testing.T) {

	table := NewTable(devTestPath)

	table.PutString("hello", "world")

	if table.Get("hello") != "world" {
		t.Error("数据不一致")
	}

	table.Delete("hello")

	log.Println(table.Get("egndfcfwownx"))

}

func TestTableFlush(t *testing.T) {

	metricChan := make(chan int, 10000)

	table := NewTable(devTestPath)

	for i := 0; i < 10; i++ {
		go func(chan int) {
			for {
				table.PutString(utils.RandStringBytesRmndr(12), utils.RandStringBytesRmndr(rand.Intn(10)+10))
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

	table := NewTable(devTestPath)

	table.lsmTree.Compaction(0)

}

package rawdb

import (
	"bytes"
	"kvdb/pkg/utils"
	"log"
	"math/rand"
	"testing"
	"time"

	"go.uber.org/zap"
)

const devTestPath = "../../build/sst"

func TestNewRawDB(t *testing.T) {

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	db := NewRawDB(devTestPath, sugar)

	db.Put([]byte("hello"), []byte("world"))

	<-time.After(600 * time.Second)
}

func TestRawDBGet(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	db := NewRawDB(devTestPath, sugar)

	db.Put([]byte("hello"), []byte("world"))

	if !bytes.Equal(db.Get([]byte("hello")), []byte("world")) {
		t.Error("数据不一致")
	}

	db.Delete("hello")

	log.Println(db.Get([]byte("egndfcfwownx")))

}

func TestRawDBFlush(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	metricChan := make(chan int, 10000)

	db := NewRawDB(devTestPath, sugar)

	for i := 0; i < 10; i++ {
		go func(chan int) {
			for {
				db.Put(utils.RandStringBytesRmndr(12), utils.RandStringBytesRmndr(rand.Intn(10)+10))
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

func TestCompactionRawDB(t *testing.T) {

	logger, _ := zap.NewDevelopment()
	sugar := logger.Sugar()
	defer sugar.Sync()

	db := NewRawDB(devTestPath, sugar)

	db.lsmTree.Compaction(0)

}

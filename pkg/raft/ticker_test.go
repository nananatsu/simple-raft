package raft

import (
	"log"
	"testing"
	"time"
)

func TestTicker(t *testing.T) {

	ticker := NewTicker()

	for i := 0; i < 5; i++ {
		<-ticker.Tick
		log.Println("触发Tick")
	}
}

func TestTickerReset(t *testing.T) {

	ticker := NewTicker()

	for i := 0; i < 5; i++ {
		<-ticker.Tick
		log.Println("触发Tick Reset")
		time.Sleep(time.Second)
		ticker.Reset()
	}

}

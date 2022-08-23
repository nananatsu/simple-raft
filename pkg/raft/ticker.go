package raft

import (
	"math"
	"time"
)

type Ticker struct {
	timeout int
	ticker  *time.Ticker

	resetc chan struct{}
	Tick   chan struct{}
}

func (t *Ticker) Start() {
	var count int
	for {
		select {
		case <-t.ticker.C:
			count++
			if count >= t.timeout {
				t.Tick <- struct{}{}
				count = 0
			}
		case <-t.resetc:
			count = 0
		}
	}
}

func (t *Ticker) Reset() {
	t.resetc <- struct{}{}
}

func (t *Ticker) SetTimeout(timeout int) {
	t.Reset()
	t.timeout = timeout
}

func NewTicker() *Ticker {

	ticker := time.NewTicker(time.Second)
	tick := make(chan struct{}, 100)
	resetc := make(chan struct{})

	t := &Ticker{
		timeout: math.MaxInt,
		ticker:  ticker,
		resetc:  resetc,
		Tick:    tick,
	}
	go t.Start()
	return t

}

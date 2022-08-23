package raft

type Storage interface {
	Put(key, value []byte)
}

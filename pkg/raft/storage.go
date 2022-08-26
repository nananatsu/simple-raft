package raft

import (
	"encoding/binary"
	"kvdb/pkg/rawdb"
)

type Storage interface {
	Append(entries []*LogEntry)
	GetTerm(index uint64) uint64
	GetFirstIndex() []byte
	GetLastIndex() []byte
}

type RaftStorage struct {
	db *rawdb.DB

	keyScratch [20]byte
}

func (rs *RaftStorage) Append(entries []*LogEntry) {
	for _, entry := range entries {
		n := binary.PutUvarint(rs.keyScratch[0:], uint64(entry.Index))
		m := binary.PutUvarint(rs.keyScratch[n:], uint64(entry.Term))
		rs.db.Put(rs.keyScratch[:n], append(rs.keyScratch[n:n+m], entry.Data...))
	}
}

func (rs *RaftStorage) GetTerm(index uint64) (term uint64) {

	n := binary.PutUvarint(rs.keyScratch[0:], index)
	value := rs.db.Get(rs.keyScratch[:n])

	if value != nil {
		term, _ = binary.Uvarint(value)
	}
	return
}

func (rs *RaftStorage) GetFirstIndex() []byte {
	return rs.db.GetMinKey()
}

func (rs *RaftStorage) GetLastIndex() []byte {
	return rs.db.GetMaxKey()
}

func (rs *RaftStorage) Snapshot() []byte {

	return nil
}

func NewRaftStorage(dir string) *RaftStorage {
	return &RaftStorage{
		db: rawdb.NewRawDB(dir),
	}
}

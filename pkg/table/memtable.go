package table

import (
	"kvdb/pkg/skiplist"
	"kvdb/pkg/wal"
)

const maxMemTableSize = 4096 * 1024

type MemTable struct {
	seqNo int
	table *skiplist.SkipList
	wal   *wal.WalWriter
}

func (m *MemTable) Get(key []byte) []byte {
	return m.table.Get(key)
}

func (m *MemTable) Put(key, value []byte) {

	if m.wal != nil {
		m.wal.Write(key, value)
	}

	m.table.Put(key, value)
}

func (m *MemTable) Size() int {
	return m.table.Size()
}

func NewMemTable(dir string, seqNo int) *MemTable {

	wal := wal.NewWalWriter(dir, seqNo)

	return &MemTable{
		seqNo: seqNo,
		table: skiplist.NewSkipList(),
		wal:   wal,
	}
}

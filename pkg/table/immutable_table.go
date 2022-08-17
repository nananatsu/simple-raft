package table

import (
	"kvdb/pkg/memtable/skiplist"
	"os"
)

type ImmutableTable struct {
	memTable *skiplist.SkipList
	writer   *SstWriter
}

func (t *ImmutableTable) FlushMemTable() {

	it := skiplist.NewSkipListIter(t.memTable)

	for it.Next() {
		t.writer.Append(it.Key, it.Value)
	}
	t.writer.Finish()
}

func NewImmutableMemTable(fd *os.File, memTable *skiplist.SkipList) *ImmutableTable {

	return &ImmutableTable{
		memTable: memTable,
		writer:   NewSstWriter(fd),
	}
}

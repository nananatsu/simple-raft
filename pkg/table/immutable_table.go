package table

import (
	"kvdb/pkg/lsm"
	"kvdb/pkg/skiplist"
	"log"
	"os"
)

type ImmutableTable struct {
	memTable *MemTable
	writer   *lsm.SstWriter
}

func (t *ImmutableTable) FlushMemTable() (int64, []byte, []*lsm.Index) {
	defer t.memTable.wal.Finish()

	log.Printf("写入: %d.%d.sst \n", 0, t.memTable.seqNo)

	it := skiplist.NewSkipListIter(t.memTable.table)

	for it.Next() {
		t.writer.Append(it.Key, it.Value)
	}
	return t.writer.Finish()
}

func NewImmutableMemTable(dir string, memTable *MemTable) *ImmutableTable {

	fd := lsm.OpenFile(dir, os.O_WRONLY|os.O_CREATE, 0, memTable.seqNo)

	immutableTable := &ImmutableTable{
		memTable: memTable,
		writer:   lsm.NewSstWriter(fd),
	}

	return immutableTable
}

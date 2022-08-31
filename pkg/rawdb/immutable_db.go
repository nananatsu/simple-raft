package rawdb

import (
	"kvdb/pkg/lsm"
	"log"
	"os"
)

type ImmutableDB struct {
	memdb  *MemDB
	writer *lsm.SstWriter
}

func (idb *ImmutableDB) Flush() (size int64, filter map[uint64][]byte, index []*lsm.Index) {

	log.Printf("写入: %d.%d.sst \n", 0, idb.memdb.SeqNo)

	it := idb.memdb.GenerateIterator()

	for it.Next() {
		idb.writer.Append(it.Key, it.Value)
	}

	size, filter, index = idb.writer.Finish()
	idb.memdb.Finish()
	return
}

func NewImmutableMemDB(dir string, memDB *MemDB) *ImmutableDB {

	fd := lsm.OpenFile(dir, os.O_WRONLY|os.O_CREATE, 0, memDB.SeqNo)

	immutableDB := &ImmutableDB{
		memdb:  memDB,
		writer: lsm.NewSstWriter(fd),
	}

	return immutableDB
}

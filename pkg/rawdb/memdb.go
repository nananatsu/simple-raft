package rawdb

import (
	"kvdb/pkg/skiplist"
	"kvdb/pkg/wal"
)

const maxMemDBSize = 4096 * 1024

type MemDB struct {
	seqNo int
	db    *skiplist.SkipList
	walw  *wal.WalWriter
}

func (mdb *MemDB) Get(key []byte) []byte {
	return mdb.db.Get(key)
}

func (mdb *MemDB) Put(key, value []byte) {
	if mdb.walw != nil {
		mdb.walw.Write(key, value)
	}
	mdb.db.Put(key, value)
}

func (mdb *MemDB) Size() int {
	return mdb.db.Size()
}

func (mdb *MemDB) Finish() {
	mdb.walw.Finish()
}

func RestoreMemDB(walFile string, dir string, seqNo int) *MemDB {

	sl := skiplist.NewSkipList()
	r := wal.NewWalReader(walFile)
	defer r.Close()

	for {
		k, v := r.Next()
		if k == nil {
			break
		}
		sl.Put(k, v)
	}

	w := wal.NewWalWriter(dir, seqNo)
	w.PaddingFile()

	return &MemDB{
		seqNo: seqNo,
		db:    sl,
		walw:  w,
	}
}

func NewMemDB(dir string, seqNo int) *MemDB {

	w := wal.NewWalWriter(dir, seqNo)
	return &MemDB{
		seqNo: seqNo,
		db:    skiplist.NewSkipList(),
		walw:  w,
	}
}

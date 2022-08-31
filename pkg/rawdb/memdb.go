package rawdb

import (
	"kvdb/pkg/skiplist"
	"kvdb/pkg/wal"
	"path"
	"strconv"

	"go.uber.org/zap"
)

const maxMemDBSize = 4096 * 1024

type MemDB struct {
	SeqNo int
	Dir   string
	db    *skiplist.SkipList
	walw  *wal.WalWriter

	logger *zap.SugaredLogger
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

func (mdb *MemDB) GenerateIterator() *skiplist.SkipListIter {
	return skiplist.NewSkipListIter(mdb.db)
}

func (mdb *MemDB) GetMin() ([]byte, []byte) {
	return mdb.db.GetMin()
}

func (mdb *MemDB) GetMax() ([]byte, []byte) {
	return mdb.db.GetMax()
}

func (mdb *MemDB) Size() int {
	return mdb.db.Size()
}

func (mdb *MemDB) Finish() {
	mdb.walw.Finish()
}

func RestoreMemDB(dir string, seqNo int, logger *zap.SugaredLogger) *MemDB {

	walFile := path.Join(dir, strconv.Itoa(0)+"."+strconv.Itoa(seqNo)+".wal")

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

	w := wal.NewWalWriter(walFile, logger)
	w.PaddingFile()

	return &MemDB{
		SeqNo:  seqNo,
		db:     sl,
		walw:   w,
		logger: logger,
	}
}

func NewMemDB(dir string, seqNo int, logger *zap.SugaredLogger) *MemDB {

	walFile := path.Join(dir, strconv.Itoa(0)+"."+strconv.Itoa(seqNo)+".wal")

	w := wal.NewWalWriter(walFile, logger)
	return &MemDB{
		SeqNo:  seqNo,
		Dir:    dir,
		db:     skiplist.NewSkipList(),
		walw:   w,
		logger: logger,
	}
}

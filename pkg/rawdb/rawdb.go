package rawdb

import (
	"io/fs"
	"kvdb/pkg/lsm"
	"kvdb/pkg/utils"
	"sync"

	"go.uber.org/zap"
)

type DB struct {
	mu sync.RWMutex

	dir     string
	memdb   *MemDB
	immdb   []*ImmutableDB
	lsmTree *lsm.Tree

	logger *zap.SugaredLogger
}

func (db *DB) Put(key, value []byte) {

	db.memdb.Put(key, value)
	if db.memdb.Size() > maxMemDBSize {
		db.flush()
	}
}

func (db *DB) Get(key []byte) []byte {

	value := db.memdb.Get(key)
	if value == nil && len(db.immdb) > 0 {
		for i := len(db.immdb) - 1; i > 0; i-- {
			value = db.immdb[i].memdb.Get(key)
			if value != nil {
				break
			}
		}
	}

	if value == nil {
		value = db.lsmTree.Get(key)
	}

	return value
}

func (db *DB) Delete(key string) {

	db.memdb.Put([]byte(key), nil)
}

func (db *DB) flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.memdb.Size() < maxMemDBSize {
		return nil
	}

	immdb := NewImmutableMemDB(db.dir, db.memdb)
	db.immdb = append(db.immdb, immdb)
	db.memdb = NewMemDB(db.dir, db.memdb.SeqNo+1, db.logger)

	if len(db.immdb) > 5 {
		db.immdb = db.immdb[len(db.immdb)-5:]
	}

	go func() {
		size, filter, index := immdb.Flush()
		db.lsmTree.AddNode(0, immdb.memdb.SeqNo, size, filter, index)
	}()

	return nil
}

func NewRawDB(dir string, logger *zap.SugaredLogger) *DB {

	lt := lsm.NewTree(dir)
	immutableDBs := make([]*ImmutableDB, 0)
	memdbs := make([]*MemDB, 0)

	maxSeqNo := 0
	callbacks := []func(int, int, string, fs.FileInfo){
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if level == 0 && seqNo > maxSeqNo {
				maxSeqNo = seqNo
			}
		},
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if subfix == "sst" && level < lsm.MaxLevel {
				if level < lsm.MaxLevel {
					lt.AddNode(level, seqNo, info.Size(), nil, nil)
				}
			}
		},
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if subfix == "wal" {
				memdbs = append(memdbs, RestoreMemDB(dir, seqNo, logger))
			}
		},
	}

	if err := utils.CheckDir(dir, callbacks); err != nil {
		logger.Infof("打开db文件夹失败", err)
	}

	var memdb *MemDB
	if len(memdbs) > 0 {
		for _, md := range memdbs {
			if md.SeqNo == maxSeqNo {
				memdb = md
			} else {
				immutableDBs = append(immutableDBs, NewImmutableMemDB(dir, md))
			}
		}
	}

	if memdb == nil {
		memdb = NewMemDB(dir, maxSeqNo+1, logger)
	}

	lt.CompactionChan <- 0

	db := &DB{dir: dir, memdb: memdb, immdb: immutableDBs, lsmTree: lt}

	if len(db.immdb) > 0 {
		for _, it := range db.immdb {
			if it.memdb.Size() > 0 {
				size, filter, index := it.Flush()
				db.lsmTree.AddNode(0, it.memdb.SeqNo, size, filter, index)
			}
		}
	}

	return db
}

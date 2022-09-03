package rawdb

import (
	"io/fs"
	"kvdb/pkg/lsm"
	"kvdb/pkg/utils"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Config struct {
	MaxMemDBSize       int
	MemDBFlushInterval time.Duration
	LSMConf            *lsm.Config
}

func NewConfig() *Config {
	lsmc := lsm.NewConfig()
	return &Config{
		MaxMemDBSize:       4096 * 1024,
		MemDBFlushInterval: 10 * time.Second,
		LSMConf:            lsmc,
	}
}

type DB struct {
	mu sync.RWMutex

	dir string

	conf    *Config
	memdb   *MemDB
	immdb   []*MemDB
	lsmTree *lsm.Tree

	logger *zap.SugaredLogger
}

func (db *DB) Put(key, value []byte) {

	db.memdb.Put(key, value)
	if db.conf.MaxMemDBSize > 0 && db.memdb.Size() > db.conf.MaxMemDBSize {
		db.Flush()
	}
}

func (db *DB) Get(key []byte) []byte {

	value := db.memdb.Get(key)
	if value == nil && len(db.immdb) > 0 {
		for i := len(db.immdb) - 1; i > 0; i-- {
			value = db.immdb[i].Get(key)
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

func (db *DB) Flush() {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.conf.MaxMemDBSize > 0 && db.memdb.Size() < db.conf.MaxMemDBSize {
		return
	}

	immdb := db.memdb
	db.immdb = append(db.immdb, immdb)
	db.memdb = NewMemDB(&MemDBConfig{Dir: db.dir, SeqNo: db.memdb.conf.SeqNo + 1, WalFlushInterval: db.conf.MemDBFlushInterval}, db.logger)

	if len(db.immdb) > 5 {
		db.immdb = db.immdb[len(db.immdb)-5:]
	}

	go func(immdb *MemDB) {
		err := db.lsmTree.FlushRecord(immdb.GenerateIterator(), 0, immdb.conf.SeqNo)
		if err != nil {
			db.logger.Errorf("将内存表写入LSM失败: %v", err)
		} else {
			immdb.Finish()
		}
	}(immdb)

}

func NewRawDB(dir string, conf *Config, logger *zap.SugaredLogger) *DB {

	lt := lsm.NewTree(dir, conf.LSMConf, logger)
	immutableDBs := make([]*MemDB, 0)
	memdbs := make([]*MemDB, 0)

	maxSeqNo := 0
	callbacks := []func(int, int, string, fs.FileInfo){
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if level == 0 && seqNo > maxSeqNo {
				maxSeqNo = seqNo
			}
		},
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if subfix == "sst" && level < conf.LSMConf.MaxLevel {
				if level < conf.LSMConf.MaxLevel {
					lt.AddNode(level, seqNo, info.Size(), nil, nil)
				}
			}
		},
		func(level, seqNo int, subfix string, info fs.FileInfo) {
			if subfix == "wal" {
				db, err := RestoreMemDB(&MemDBConfig{Dir: dir, SeqNo: seqNo, WalFlushInterval: conf.MemDBFlushInterval}, logger)
				if err != nil {
					logger.Errorf("还原memdb失败:%v", err)
				} else {
					memdbs = append(memdbs, db)
				}
			}
		},
	}

	if err := utils.CheckDir(dir, callbacks); err != nil {
		logger.Infof("打开db文件夹失败", err)
	}

	var memdb *MemDB
	if len(memdbs) > 0 {
		for _, md := range memdbs {
			if md.conf.SeqNo == maxSeqNo {
				memdb = md
			} else {
				immutableDBs = append(immutableDBs, md)
			}
		}
	}

	if memdb == nil {
		memdb = NewMemDB(&MemDBConfig{Dir: dir, SeqNo: maxSeqNo + 1, WalFlushInterval: conf.MemDBFlushInterval}, logger)
	}

	lt.Compacc <- 0
	db := &DB{dir: dir, memdb: memdb, immdb: immutableDBs, lsmTree: lt, conf: conf}

	if len(db.immdb) > 0 {
		for _, it := range db.immdb {
			if it.Size() > 0 {
				db.lsmTree.FlushRecord(db.memdb.GenerateIterator(), 0, it.conf.SeqNo)
			}
		}
	}

	return db
}

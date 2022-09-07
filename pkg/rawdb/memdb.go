package rawdb

import (
	"kvdb/pkg/skiplist"
	"kvdb/pkg/wal"
	"os"
	"path"
	"strconv"
	"time"

	"go.uber.org/zap"
)

type MemDBConfig struct {
	Dir              string
	SeqNo            int
	WalFlushInterval time.Duration
}

type MemDB struct {
	conf   *MemDBConfig
	db     *skiplist.SkipList
	walw   *wal.WalWriter
	ticker *time.Ticker
	stop   chan struct{}
	logger *zap.SugaredLogger
}

func (db *MemDB) Get(key []byte) []byte {
	return db.db.Get(key)
}

func (db *MemDB) Put(key, value []byte) {
	if db.walw != nil {
		db.walw.Write(key, value)
	}
	db.db.Put(key, value)
}

func (db *MemDB) GenerateIterator() *skiplist.SkipListIter {
	return skiplist.NewSkipListIter(db.db)
}

func (db *MemDB) GetMax() ([]byte, []byte) {
	return db.db.GetMax()
}

func (db *MemDB) GetRange(start, end []byte) [][][]byte {
	return db.db.GetRange(start, end)
}

func (db *MemDB) Size() int {
	return db.db.Size()
}

func (db *MemDB) GetSeqNo() int {
	return db.conf.SeqNo
}

func (db *MemDB) GetDir() string {
	return db.conf.Dir
}

func (db *MemDB) Sync() {
	go func() {
		for {
			select {
			case <-db.ticker.C:
				if db.walw != nil {
					db.walw.Flush()
				}
			case <-db.stop:
				if db.walw != nil {
					db.walw.Flush()
				}
				return
			}
		}
	}()
}

func (db *MemDB) Finish() {
	db.stop <- struct{}{}
	if db.walw != nil {
		db.walw.Finish()
	}
	db.ticker.Stop()
	close(db.stop)
}

func RestoreMemDB(conf *MemDBConfig, logger *zap.SugaredLogger) (*MemDB, error) {

	walFile := path.Join(conf.Dir, strconv.Itoa(0)+"."+strconv.Itoa(conf.SeqNo)+".wal")
	fd, err := os.OpenFile(walFile, os.O_RDONLY, 0644)
	if err != nil {
		logger.Errorf("打开预写日志文件%d失败: %v", walFile, err)
		return nil, err
	}

	sl := skiplist.NewSkipList()
	r := wal.NewWalReader(fd, logger)
	defer r.Close()

	for {
		k, v := r.Next()
		if len(k) == 0 {
			break
		}
		sl.Put(k, v)
	}

	wfd, err := os.OpenFile(walFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf("打开预写日志文件%d失败: %v", walFile, err)
		// return nil, err
	}

	w := wal.NewWalWriter(wfd, logger)
	w.PaddingFile()

	db := &MemDB{
		conf:   conf,
		db:     sl,
		walw:   w,
		ticker: time.NewTicker(conf.WalFlushInterval),
		logger: logger,
	}
	db.Sync()
	return db, nil
}

func NewMemDB(conf *MemDBConfig, logger *zap.SugaredLogger) *MemDB {

	walFile := path.Join(conf.Dir, strconv.Itoa(0)+"."+strconv.Itoa(conf.SeqNo)+".wal")
	fd, err := os.OpenFile(walFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf("打开预写日志文件%d失败: %v", walFile, err)
	}

	db := &MemDB{
		conf:   conf,
		db:     skiplist.NewSkipList(),
		walw:   wal.NewWalWriter(fd, logger),
		ticker: time.NewTicker(conf.WalFlushInterval),
		stop:   make(chan struct{}),
		logger: logger,
	}

	db.Sync()
	return db
}

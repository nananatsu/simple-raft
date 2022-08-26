package rawdb

import (
	"bytes"
	"io/fs"
	"kvdb/pkg/lsm"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	mu sync.RWMutex

	dir     string
	memdb   *MemDB
	immdb   []*ImmutableDB
	lsmTree *lsm.Tree
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
	db.memdb = NewMemDB(db.dir, db.memdb.seqNo+1)

	if len(db.immdb) > 5 {
		db.immdb = db.immdb[len(db.immdb)-5:]
	}

	go func() {
		size, filter, index := immdb.Flush()
		db.lsmTree.AddNode(0, immdb.memdb.seqNo, size, filter, index)
	}()

	return nil
}

func (db *DB) GetMinKey() (key []byte) {
	minTreeKey := db.lsmTree.GetMinKey()
	minMemKey, _ := db.memdb.db.GetMin()

	if minMemKey == nil && minTreeKey != nil {
		key = minTreeKey
	} else if minMemKey != nil && minTreeKey == nil {
		key = minMemKey
	} else if bytes.Compare(minMemKey, minTreeKey) < 0 {
		key = minMemKey
	} else {
		key = minTreeKey
	}

	return
}

func (db *DB) GetMaxKey() (key []byte) {
	maxTreeKey := db.lsmTree.GetMaxKey()
	maxMemKey, _ := db.memdb.db.GetMax()

	if maxTreeKey == nil && maxMemKey != nil {
		key = maxMemKey
	} else if maxTreeKey != nil && maxMemKey == nil {
		key = maxTreeKey
	} else if bytes.Compare(maxMemKey, maxTreeKey) < 0 {
		key = maxTreeKey
	} else {
		key = maxMemKey
	}

	return
}

func NewRawDB(dir string) *DB {

	lt := lsm.NewTree(dir)

	entries, err := os.ReadDir(dir)

	if err != nil {
		log.Println("打开db文件夹失败", err)
	}

	immutableDBs := make([]*ImmutableDB, 0)
	memdbs := make([]*MemDB, 0)

	maxSeqNo := 0
	for _, entry := range entries {

		level, seqNo, fileType, fileInfo := CheckFiles(entry)

		if level == -1 {
			continue
		}

		if fileInfo.Size() == 0 {
			os.Remove(path.Join(dir, entry.Name()))
			continue
		}

		if level == 0 && seqNo > maxSeqNo {
			maxSeqNo = seqNo
		}

		if fileType == "sst" {
			if level < lsm.MaxLevel {
				lt.AddNode(level, seqNo, fileInfo.Size(), nil, nil)
			}
		} else if fileType == "wal" {
			memdbs = append(memdbs, RestoreMemDB(path.Join(dir, entry.Name()), dir, seqNo))
		}
	}

	var memdb *MemDB
	if len(memdbs) > 0 {
		for _, md := range memdbs {
			if md.seqNo == maxSeqNo {
				memdb = md
			} else {
				immutableDBs = append(immutableDBs, NewImmutableMemDB(dir, md))
			}
		}
	}

	if memdb == nil {
		memdb = NewMemDB(dir, maxSeqNo+1)
	}

	lt.CompactionChan <- 0

	db := &DB{dir: dir, memdb: memdb, immdb: immutableDBs, lsmTree: lt}

	if len(db.immdb) > 0 {
		for _, it := range db.immdb {
			if it.memdb.Size() > 0 {
				size, filter, index := it.Flush()
				db.lsmTree.AddNode(0, it.memdb.seqNo, size, filter, index)
			}
		}
	}

	return db
}

func CheckFiles(entry fs.DirEntry) (int, int, string, fs.FileInfo) {

	if entry.IsDir() {
		return -1, -1, "", nil
	}

	levelNodeInfo := strings.Split(entry.Name(), ".")

	if len(levelNodeInfo) != 3 {
		return -1, -1, "", nil
	}

	level, err := strconv.Atoi(levelNodeInfo[0])

	if err != nil {
		return -1, -1, "", nil
	}

	seqNo, err := strconv.Atoi(levelNodeInfo[1])

	if err != nil {
		return -1, -1, "", nil
	}

	fileInfo, err := entry.Info()

	if err != nil {
		return -1, -1, "", nil
	}

	return level, seqNo, levelNodeInfo[2], fileInfo
}

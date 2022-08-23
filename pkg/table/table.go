package table

import (
	"io/fs"
	"kvdb/pkg/lsm"
	"kvdb/pkg/wal"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type Table struct {
	mu sync.RWMutex

	dir            string
	memTable       *MemTable
	immutableTable []*ImmutableTable
	lsmTree        *lsm.Tree
}

func (t *Table) PutString(key, value string) {
	t.Put([]byte(key), []byte(value))
}

func (t *Table) Put(key, value []byte) {

	t.memTable.Put(key, value)
	if t.memTable.Size() > maxMemTableSize {
		t.flush()
	}
}

func (t *Table) Get(key string) string {

	k := []byte(key)

	value := t.memTable.Get(k)

	if value == nil && len(t.immutableTable) > 0 {
		for i := len(t.immutableTable) - 1; i > 0; i-- {
			value = t.immutableTable[i].memTable.Get(k)
			if value != nil {
				break
			}
		}
	}

	if value == nil {
		value = t.lsmTree.Get(k)
	}

	return string(value)
}

func (t *Table) Delete(key string) {

	t.memTable.Put([]byte(key), nil)
}

func (t *Table) flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.memTable.Size() < maxMemTableSize {
		return nil
	}

	immutableTable := NewImmutableMemTable(t.dir, t.memTable)
	t.immutableTable = append(t.immutableTable, immutableTable)
	t.memTable = NewMemTable(t.dir, t.memTable.seqNo+1)

	if len(t.immutableTable) > 5 {
		t.immutableTable = t.immutableTable[len(t.immutableTable)-5:]
	}

	go func() {
		size, filter, index := immutableTable.FlushMemTable()
		t.lsmTree.AddNode(0, immutableTable.memTable.seqNo, size, filter, index)
	}()

	return nil
}

func NewTable(dir string) *Table {

	lt := lsm.NewTree(dir)

	entries, err := os.ReadDir(dir)

	if err != nil {
		log.Println("打开db文件夹失败", err)
	}

	immutableTables := make([]*ImmutableTable, 0)

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
			memTable := NewMemTable(dir, seqNo)
			r := wal.NewWalReader(path.Join(dir, entry.Name()))

			for {
				k, v := r.Next()
				if k == nil {
					break
				}
				memTable.Put(k, v)
			}
			immutableTables = append(immutableTables, NewImmutableMemTable(dir, memTable))
		}
	}

	lt.CompactionChan <- 0

	table := &Table{dir: dir, memTable: NewMemTable(dir, maxSeqNo+1), immutableTable: immutableTables, lsmTree: lt}

	if len(table.immutableTable) > 0 {
		for _, it := range table.immutableTable {
			size, filter, index := it.FlushMemTable()
			table.lsmTree.AddNode(0, it.memTable.seqNo, size, filter, index)
		}
	}

	return table
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

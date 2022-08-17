package table

import (
	"bytes"
	"kvdb/pkg/memtable/skiplist"
	"log"
	"sync"
)

type Table struct {
	mu sync.RWMutex

	Path      string
	memTable  *skiplist.SkipList
	levelTree *LevelTree
}

func (t *Table) Put(key, value string) error {

	t.memTable.Put([]byte(key), []byte(value))

	if t.memTable.Size() > maxMemTableSize {
		t.flush()
	}

	return nil
}

func (t *Table) flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.memTable.Size() < maxMemTableSize {
		return nil
	}

	fd, fileName, seqNo := t.levelTree.NewSstFile(0)
	immutableTable := NewImmutableMemTable(fd, t.memTable)
	t.memTable = skiplist.NewMemTable()

	go func() {

		log.Println("写入", fileName)
		immutableTable.FlushMemTable()
		t.levelTree.AddLevelNode(fileName, 0, seqNo)

		t.levelTree.compactionChan <- 0
	}()

	return nil
}

func NewTable(filePath string) *Table {

	return &Table{Path: filePath, memTable: skiplist.NewMemTable(), levelTree: NewLevelTree(filePath)}
}

type Record struct {
	Key   []byte
	Value []byte
	Idx   int
	next  *Record
}

func (r *Record) push(key, value []byte, idx int) *Record {

	h := r

	cur := r
	var prev *Record

	for {
		if cur == nil {
			if prev != nil {
				prev.next = &Record{Key: key, Value: value, Idx: idx}
			} else {
				h = &Record{Key: key, Value: value, Idx: idx}
			}
			break
		}

		cmp := bytes.Compare(key, cur.Key)
		if cmp == 0 {
			if idx >= r.Idx {
				cur.Key = key
				cur.Value = value
				cur.Idx = idx
			}
			break
		} else if cmp < 0 {
			if prev != nil {
				prev.next = &Record{Key: key, Value: value, Idx: idx}
				prev.next.next = cur
			} else {
				h = &Record{Key: key, Value: value, Idx: idx}
				h.next = cur
			}
			break
		} else {
			prev = cur
			cur = cur.next
		}
	}

	return h
}

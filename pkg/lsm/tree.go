package lsm

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"strconv"
	"sync"
)

const MaxLevel = 7
const SstSize = 4096 * 1024

type Tree struct {
	mu sync.RWMutex

	dir            string
	tree           [][]*Node
	seqNo          []int
	CompactionChan chan int
}

func (t *Tree) Insert(ln *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()

	level := ln.Level
	length := len(t.tree[level]) - 1
	idx := length
	for ; idx >= 0; idx-- {
		if ln.SeqNo > t.tree[level][idx].SeqNo {
			break
		} else if ln.SeqNo == t.tree[level][idx].SeqNo {
			t.tree[level][idx] = ln
			return
		}
	}

	if idx == length {
		t.tree[level] = append(t.tree[level], ln)
	} else {
		var newLevel []*Node
		if idx == -1 {
			newLevel = make([]*Node, 1)
			newLevel = append(newLevel, t.tree[level]...)
		} else {
			newLevel = append(t.tree[level][:idx+1], t.tree[level][idx:]...)
		}
		newLevel[idx+1] = ln
		t.tree[level] = newLevel
	}
}

func (t *Tree) AddNode(level, seqNo int, size int64, filter map[uint64][]byte, index []*Index) *Node {

	fd := OpenFile(t.dir, os.O_RDONLY, level, seqNo)
	var ln *Node

	if filter == nil {
		ln = &Node{
			sr:       NewSstReader(fd),
			Level:    level,
			SeqNo:    seqNo,
			curBlock: 1,
		}

		if t.seqNo[level] < seqNo {
			t.seqNo[level] = seqNo
		}

		if ln.Load() != nil {
			return nil
		}
		t.Insert(ln)
	} else {
		ln = &Node{
			sr:       NewSstReader(fd),
			Level:    level,
			SeqNo:    seqNo,
			curBlock: 1,
			FileSize: size,
			filter:   filter,
			index:    index,
			startKey: index[0].Key,
			endKey:   index[len(index)-1].Key,
		}
		t.Insert(ln)

		t.CompactionChan <- level
	}

	return ln
}

func (t *Tree) RemoveNode(nodes []*Node) {
	t.mu.Lock()
	defer t.mu.Unlock()

	lv := nodes[0].Level

	for _, node := range nodes {
		node.Destory()
	}

	for i := lv; i <= lv+1; i++ {
		newLevel := make([]*Node, 0)
		for _, node := range t.tree[i] {
			if node.Level != -1 {
				newLevel = append(newLevel, node)
			}
		}
		t.tree[i] = newLevel
	}
}

func (t *Tree) NextSeqNo(level int) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seqNo[level]++

	return t.seqNo[level]
}

func (t *Tree) Compaction(level int) error {

	nodes := t.PickupCompactionNode(level)

	if len(nodes) == 0 {
		return nil
	}

	nextLevel := level + 1
	seqNo := t.NextSeqNo(nextLevel)

	fd := OpenFile(t.dir, os.O_WRONLY|os.O_CREATE, nextLevel, seqNo)
	writer := NewSstWriter(fd)

	maxNodeSize := SstSize * int(math.Pow10(nextLevel))

	var record *Record
	var files string

	for i, node := range nodes {
		files += fmt.Sprintf("%d.%d.sst ", node.Level, node.SeqNo)
		k, v := node.NextRecord()
		record = record.push(k, v, i)
	}
	log.Println("合并", files)

	for record != nil {
		i := record.Idx
		writer.Append(record.Key, record.Value)
		record = record.next
		k, v := nodes[i].NextRecord()
		if k != nil {
			record = record.push(k, v, i)
		}

		if writer.Size() > maxNodeSize {
			size, filter, index := writer.Finish()
			writer.Close()

			t.AddNode(nextLevel, seqNo, size, filter, index)

			seqNo = t.NextSeqNo(nextLevel)
			fd = OpenFile(t.dir, os.O_WRONLY|os.O_CREATE, nextLevel, seqNo)
			writer = NewSstWriter(fd)
		}

	}
	size, filter, index := writer.Finish()
	t.AddNode(nextLevel, seqNo, size, filter, index)

	t.RemoveNode(nodes)

	return nil
}

func (t *Tree) PickupCompactionNode(level int) []*Node {
	t.mu.Lock()
	defer t.mu.Unlock()

	compactionNode := make([]*Node, 0)

	if len(t.tree[level]) == 0 {
		return compactionNode
	}

	startKey := t.tree[level][0].startKey
	endKey := t.tree[level][0].endKey

	if level != 0 {
		node := t.tree[level][(len(t.tree[level])-1)/2]
		if bytes.Compare(node.startKey, startKey) < 0 {
			startKey = node.startKey
		}
		if bytes.Compare(node.endKey, endKey) > 0 {
			endKey = node.endKey
		}
	}

	for i := level; i <= level+1; i++ {
		for _, node := range t.tree[i] {

			if node.index == nil {
				continue
			}
			nodeStartKey := node.index[0].Key
			nodeEndKey := node.index[len(node.index)-1].Key

			// log.Println("Level:", i, "SeqNum:", node.SeqNo, "Start:", string(nodeStartKey), "End:", string(nodeEndKey))
			if bytes.Compare(startKey, nodeEndKey) <= 0 && bytes.Compare(endKey, nodeStartKey) >= 0 && !node.compacting {
				compactionNode = append(compactionNode, node)
				node.compacting = true
			}
		}
	}

	return compactionNode
}

func (t *Tree) CheckCompaction() {

	levelChan := make([]chan int, MaxLevel)

	for i := 0; i < MaxLevel; i++ {

		ch := make(chan int, 100)
		levelChan[i] = ch

		if i == 0 {
			go func(ch chan int) {
				for {
					<-ch
					if len(t.tree[0]) > 4 {
						t.Compaction(0)
					}
				}
			}(ch)

		} else {

			go func(ch chan int, lv int) {
				for {
					<-ch
					var prevSize int64
					maxNodeSize := int64(SstSize * math.Pow10(lv+1))
					for {
						var totalSize int64
						for _, node := range t.tree[lv] {
							totalSize += node.FileSize
						}
						// log.Printf("Level %d 当前大小: %d M, 最大大小: %d M\n", lv, totalSize/(1024*1024), maxNodeSize/(1024*1024))

						if totalSize > maxNodeSize && (prevSize == 0 || totalSize < prevSize) {
							t.Compaction(lv)
							prevSize = totalSize
						} else {
							break
						}
					}
				}
			}(ch, i)
		}
	}

	for {
		lv := <-t.CompactionChan

		// log.Printf("检查Level : %d是否可合并,待执行检查数量 : %d", lv, len(lt.compactionChan))

		levelChan[lv] <- 1
	}
}

func (t *Tree) Get(key []byte) (value []byte) {

	for _, nodes := range t.tree {
		for i := len(nodes) - 1; i >= 0; i-- {

			value = nodes[i].Get(key)
			if value != nil {
				return
			}
		}
	}
	return
}

func (t *Tree) GetMinKey() (key []byte) {

	for i := len(t.tree) - 1; i >= 0; i-- {
		if (len(t.tree[i])) == 0 {
			continue
		}
		if key == nil {
			key = t.tree[i][0].startKey
		}

		if bytes.Compare(t.tree[i][0].startKey, key) < 0 {
			key = t.tree[i][0].startKey
		}

		if i == 0 {
			for _, n := range t.tree[i][1:] {
				if bytes.Compare(n.startKey, key) < 0 {
					key = n.startKey
				}
			}
		}
	}

	return
}

func (t *Tree) GetMaxKey() (key []byte) {

	for i := len(t.tree) - 1; i >= 0; i-- {

		if (len(t.tree[i])) == 0 {
			continue
		}

		if key == nil {
			key = t.tree[i][len(t.tree[i])-1].endKey
		}

		if bytes.Compare(t.tree[i][len(t.tree[i])-1].endKey, key) > 0 {
			key = t.tree[i][len(t.tree[i])-1].endKey
		}

		if i == 0 {
			for _, n := range t.tree[0][1:] {
				if bytes.Compare(n.endKey, key) > 0 {
					key = n.endKey
				}
			}
		}
	}
	return
}

func NewTree(dir string) *Tree {

	compactionChan := make(chan int, 100)

	levelTree := make([][]*Node, MaxLevel)

	for i := range levelTree {
		levelTree[i] = make([]*Node, 0)
	}

	seqNos := make([]int, MaxLevel)

	lt := &Tree{
		dir:            dir,
		tree:           levelTree,
		seqNo:          seqNos,
		CompactionChan: compactionChan,
	}

	// for i:=0;i< runtime.NumCPU() -1;i++{
	go lt.CheckCompaction()
	// }
	return lt
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

func OpenFile(dir string, flag, level, seqNo int) *os.File {

	fd, err := os.OpenFile(path.Join(dir, strconv.Itoa(level)+"."+strconv.Itoa(seqNo)+".sst"), flag, 0644)

	if err != nil {
		log.Println("打开文件失败", err)
	}

	return fd
}

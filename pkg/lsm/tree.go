package lsm

import (
	"bytes"
	"fmt"
	"kvdb/pkg/skiplist"
	"math"
	"os"
	"path"
	"strconv"
	"sync"

	"go.uber.org/zap"
)

type Config struct {
	MaxLevel            int
	SstSize             int
	SstDataBlockSize    int
	SstFooterSize       int
	SstBlockTrailerSize int
	SstRestartInterval  int
}

func NewConfig() *Config {
	return &Config{
		MaxLevel:            7,
		SstSize:             4096 * 1024,
		SstDataBlockSize:    16 * 1024,
		SstFooterSize:       40,
		SstBlockTrailerSize: 4,
		SstRestartInterval:  16,
	}
}

type Tree struct {
	mu      sync.RWMutex
	dir     string
	conf    *Config
	tree    [][]*Node
	seqNo   []int
	compacc chan int
	stopc   chan struct{}
	logger  *zap.SugaredLogger
}

func (t *Tree) Close() {
	t.stopc <- struct{}{}
	close(t.stopc)
	close(t.compacc)
	for _, level := range t.tree {
		for _, n := range level {
			n.Close()
		}
	}
}

func (t *Tree) FlushRecord(it *skiplist.SkipListIter, level, seqNo int) error {

	fd, err := OpenFile(t.dir, os.O_WRONLY|os.O_CREATE, level, seqNo)
	if err != nil {
		t.logger.Errorf("创建 %d.%d.sst 失败: %v", level, seqNo, err)
		return err
	}

	w := NewSstWriter(fd, t.conf, t.logger)

	count := 0
	for it.Next() {
		w.Append(it.Key, it.Value)
		count++
	}

	t.logger.Infof("写入: %d.%d.sst,数据数: %d ", level, seqNo, count)

	size, filter, index := w.Finish()
	t.AddNode(level, seqNo, size, filter, index)

	return nil
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

func (t *Tree) AddNode(level, seqNo int, size int64, filter map[uint64][]byte, index []*Index) {

	fd, err := OpenFile(t.dir, os.O_RDONLY, level, seqNo)
	if err != nil {
		t.logger.Errorf("无法加入节点，打开 %d,%d.sst文件失败:%v", level, seqNo, err)
		return
	}

	var ln *Node

	if filter == nil {
		ln = &Node{
			sr:       NewSstReader(fd, t.conf),
			Level:    level,
			SeqNo:    seqNo,
			curBlock: 1,
			logger:   t.logger,
		}

		if t.seqNo[level] < seqNo {
			t.seqNo[level] = seqNo
		}

		if ln.Load() != nil {
			return
		}
		t.Insert(ln)
	} else {
		ln = &Node{
			sr:       NewSstReader(fd, t.conf),
			Level:    level,
			SeqNo:    seqNo,
			curBlock: 1,
			FileSize: size,
			filter:   filter,
			index:    index,
			startKey: index[0].Key,
			endKey:   index[len(index)-1].Key,
			logger:   t.logger,
		}
		t.Insert(ln)
		t.compacc <- level
	}
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

	fd, err := OpenFile(t.dir, os.O_WRONLY|os.O_CREATE, nextLevel, seqNo)
	if err != nil {
		t.logger.Errorf("无法合并lsm日志,打开/创建 %d,%d.sst文件失败:%v", nextLevel, seqNo, err)
		return err
	}

	writer := NewSstWriter(fd, t.conf, t.logger)

	maxNodeSize := t.conf.SstSize * int(math.Pow10(nextLevel))

	var record *Record
	var files string
	writeCount := 0

	for i, node := range nodes {
		files += fmt.Sprintf("%d.%d.sst ", node.Level, node.SeqNo)
		record = record.Fill(nodes, i)
	}
	t.logger.Debugf("合并: %v", files)

	for record != nil {
		writeCount++
		i := record.Idx
		writer.Append(record.Key, record.Value)
		record = record.next.Fill(nodes, i)

		if writer.Size() > maxNodeSize {
			size, filter, index := writer.Finish()
			writer.Close()

			t.logger.Infof("写入: %d.%d.sst, 数据数: %d ", nextLevel, seqNo, writeCount)
			writeCount = 0

			t.AddNode(nextLevel, seqNo, size, filter, index)
			seqNo = t.NextSeqNo(nextLevel)
			fd, err = OpenFile(t.dir, os.O_WRONLY|os.O_CREATE, nextLevel, seqNo)
			if err != nil {
				t.logger.Errorf("打开/创建 %d,%d.sst文件失败:%v", nextLevel, seqNo, err)
				return err
			}
			writer = NewSstWriter(fd, t.conf, t.logger)
		}

	}
	size, filter, index := writer.Finish()

	t.logger.Infof("写入: %d.%d.sst, 数据数: %d ", nextLevel, seqNo, writeCount)
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

			// t.logger.infoln("Level:", i, "SeqNum:", node.SeqNo, "Start:", string(nodeStartKey), "End:", string(nodeEndKey))
			if bytes.Compare(startKey, nodeEndKey) <= 0 && bytes.Compare(endKey, nodeStartKey) >= 0 && !node.compacting {
				compactionNode = append(compactionNode, node)
				node.compacting = true
			}
		}
	}

	return compactionNode
}

func (t *Tree) CheckCompaction() {

	level0 := make(chan struct{}, 100)
	levelN := make(chan int, 100)

	go func() {
		for {
			select {
			case <-level0:
				if len(t.tree[0]) > 4 {
					t.Compaction(0)
				}
			case <-t.stopc:
				close(level0)
				return
			}

		}
	}()

	go func() {
		for {
			select {
			case lv := <-levelN:
				var prevSize int64
				maxNodeSize := int64(t.conf.SstSize * int(math.Pow10(lv+1)))
				for {
					var totalSize int64
					for _, node := range t.tree[lv] {
						totalSize += node.FileSize
					}
					// t.logger.infof("Level %d 当前大小: %d M, 最大大小: %d M\n", lv, totalSize/(1024*1024), maxNodeSize/(1024*1024))

					if totalSize > maxNodeSize && (prevSize == 0 || totalSize < prevSize) {
						t.Compaction(lv)
						prevSize = totalSize
					} else {
						break
					}
				}
			case <-t.stopc:
				close(levelN)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-t.stopc:
				return
			case lv := <-t.compacc:
				if lv == 0 {
					level0 <- struct{}{}
				} else {
					levelN <- lv
				}
			}
		}
	}()
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

func NewTree(dir string, conf *Config, logger *zap.SugaredLogger) *Tree {

	compactionChan := make(chan int, 100)

	levelTree := make([][]*Node, conf.MaxLevel)

	for i := range levelTree {
		levelTree[i] = make([]*Node, 0)
	}

	seqNos := make([]int, conf.MaxLevel)

	lt := &Tree{
		dir:     dir,
		conf:    conf,
		tree:    levelTree,
		seqNo:   seqNos,
		compacc: compactionChan,
		stopc:   make(chan struct{}),
		logger:  logger,
	}

	// for i:=0;i< runtime.NumCPU() -1;i++{
	lt.CheckCompaction()
	// }
	return lt
}

type Record struct {
	Key   []byte
	Value []byte
	Idx   int
	next  *Record
}

func (r *Record) Fill(source []*Node, idx int) *Record {
	record := r

	k, v := source[idx].NextRecord()
	if k != nil {
		record, idx = record.push(k, v, idx)

		for idx > -1 {
			k, v := source[idx].NextRecord()
			if k != nil {
				record, idx = record.push(k, v, idx)
			} else {
				idx = -1
			}
		}
	}

	return record
}

func (r *Record) push(key, value []byte, idx int) (*Record, int) {

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
				old := cur.Idx
				cur.Key = key
				cur.Value = value
				cur.Idx = idx
				return h, old
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
	return h, -1
}

func OpenFile(dir string, flag, level, seqNo int) (*os.File, error) {

	fd, err := os.OpenFile(path.Join(dir, strconv.Itoa(level)+"."+strconv.Itoa(seqNo)+".sst"), flag, 0644)

	if err != nil {
		return nil, err
	}

	return fd, nil
}

package skiplist

import (
	"bytes"
	"math/rand"
	"sync"
)

const tMaxHeight = 12

const (
	nKV = iota
	nKey
	nVal
	nHeight
	nNext
)

type SkipListIter struct {
	sl *SkipList

	node       int
	Key, Value []byte
}

func (i *SkipListIter) Next() bool {

	i.node = i.sl.kvNode[i.node+nNext]

	if i.node != 0 {
		keyStart := i.sl.kvNode[i.node]
		keyEnd := keyStart + i.sl.kvNode[i.node+nKey]
		valueEnd := keyEnd + i.sl.kvNode[i.node+nVal]

		i.Key = i.sl.kvData[keyStart:keyEnd]
		i.Value = i.sl.kvData[keyEnd:valueEnd]

		return true
	}
	return false
}

func NewSkipListIter(sl *SkipList) *SkipListIter {
	return &SkipListIter{sl: sl}
}

type SkipList struct {
	mu   sync.RWMutex
	rand *rand.Rand

	kvData []byte
	// 0 kvData 偏移
	// 1 key长度
	// 2 value长度
	// 3 链表层数
	// 3...链表层数 下一节点
	kvNode    []int
	maxHeight int
	prevNode  [tMaxHeight]int
	kvSize    int
}

func (s *SkipList) randHeight() (h int) {
	const branching = 4
	h = 1
	for h < tMaxHeight && s.rand.Int()%branching == 0 {
		h++
	}
	return
}

func (s *SkipList) getNode(key []byte) (int, bool) {
	n := 0
	h := s.maxHeight - 1

	for {
		// h := t.kvNode[n+nHeight] - 1
		next := s.kvNode[n+nNext+h]
		cmp := 1

		if next != 0 {
			keyStart := s.kvNode[next]
			keyLen := s.kvNode[next+nKey]

			cmp = bytes.Compare(s.kvData[keyStart:keyStart+keyLen], key)
		}

		if cmp == 0 {
			return next, true
		} else if cmp < 0 {
			n = next
		} else {
			s.prevNode[h] = n
			if h > 0 {
				h--
			} else {
				return next, false
			}
		}

	}
}

func (s *SkipList) Put(key []byte, value []byte) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	n, b := s.getNode(key)

	keyStart := len(s.kvData)
	s.kvData = append(s.kvData, key...)
	s.kvData = append(s.kvData, value...)

	if b {
		s.kvSize += len(value) - s.kvNode[n+nVal]

		s.kvNode[n] = keyStart
		s.kvNode[n+nVal] = len(value)

		return nil
	}

	h := s.randHeight()

	if h > s.maxHeight {
		for i := s.maxHeight; i < h; i++ {
			s.prevNode[i] = 0
		}
		s.maxHeight = h
	}

	n = len(s.kvNode)
	s.kvNode = append(s.kvNode, keyStart, len(key), len(value), h)
	for i, node := range s.prevNode[:h] {
		m := node + nNext + i
		s.kvNode = append(s.kvNode, s.kvNode[m])
		s.kvNode[m] = n
	}

	s.kvSize += len(value) + len(key)

	return nil
}

func (s *SkipList) Get(key []byte) []byte {

	s.mu.RLock()
	defer s.mu.RUnlock()

	n, b := s.getNode(key)

	if b {
		keyStart := s.kvNode[n]
		keyLen := s.kvNode[n+nKey]
		valueLen := s.kvNode[n+nVal]

		return s.kvData[keyStart+keyLen : keyStart+keyLen+valueLen]
	} else {
		return nil
	}

}

func (s *SkipList) Size() int {
	return s.kvSize
}

func NewMemTable() *SkipList {

	s := &SkipList{
		rand:      rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		kvData:    make([]byte, 0),
		kvNode:    make([]int, 4+tMaxHeight),
	}

	s.kvNode[nHeight] = tMaxHeight

	return s
}

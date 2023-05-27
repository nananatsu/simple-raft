package skiplist

import (
	"bytes"
	"kvdb/pkg/utils"
	"math/rand"
	"sync"
)

// 跳表最大高度
const tMaxHeight = 12

const (
	nKV     = iota
	nKey    // key偏移
	nVal    // value偏移
	nHeight // 高度偏移
	nNext   // 下一跳位置偏移
)

type SkipListIter struct {
	sl         *SkipList
	node       int    // 当前位置
	Key, Value []byte // 键值对数据
}

// 获取下一条数据
func (i *SkipListIter) Next() bool {

	// 下一跳数据
	i.node = i.sl.kvNode[i.node+nNext]
	// 存在下一条数据
	if i.node != 0 {
		// 解析键值数据
		keyStart := i.sl.kvNode[i.node]
		keyEnd := keyStart + i.sl.kvNode[i.node+nKey]
		valueEnd := keyEnd + i.sl.kvNode[i.node+nVal]

		i.Key = i.sl.kvData[keyStart:keyEnd]
		i.Value = i.sl.kvData[keyEnd:valueEnd]
		return true
	}
	return false
}

// 将跳表包装为迭代器
func NewSkipListIter(sl *SkipList) *SkipListIter {
	return &SkipListIter{sl: sl}
}

type SkipList struct {
	mu     sync.RWMutex
	rand   *rand.Rand // 随机函数,判断数据是否存在于某层
	kvData []byte     // 跳表实际数据
	// 0 kvData 偏移
	// 1 key长度
	// 2 value长度
	// 3 高度
	// 4 ... 4+h-1 各高度下一跳位置
	kvNode    []int
	maxHeight int             // 最大高度
	prevNode  [tMaxHeight]int // 上一跳位置,查询数据时回溯
	kvSize    int             // 数据大小
}

// 随机高度
func (s *SkipList) randHeight() (h int) {
	const branching = 4
	h = 1
	for h < tMaxHeight && s.rand.Int()%branching == 0 {
		h++
	}
	return
}

// 获取键位置、或键前一跳
func (s *SkipList) getNode(key []byte) (int, bool) {
	n := 0
	h := s.maxHeight - 1

	for {
		// 获取下一跳位置
		next := s.kvNode[n+nNext+h]
		cmp := 1

		// 下一跳存在，获取键与请求键比较
		if next != 0 {
			keyStart := s.kvNode[next]
			keyLen := s.kvNode[next+nKey]
			cmp = bytes.Compare(s.kvData[keyStart:keyStart+keyLen], key)
		}

		// 找到请求键
		if cmp == 0 {
			return next, true
		} else if cmp < 0 { // 当前键小于请求键,继续下一跳
			n = next
		} else { // 当前键大于请求键，不前进到下一跳，降低高度查询
			s.prevNode[h] = n
			if h > 0 {
				h--
			} else { // 已到最底层，请求键不存在
				return next, false
			}
		}

	}
}

// 添加键值对到跳表
func (s *SkipList) Put(key []byte, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lenv := len(value)
	lenk := len(key)
	// 查询键位置
	n, b := s.getNode(key)
	keyStart := len(s.kvData)
	// 追加键值对
	s.kvData = append(s.kvData, key...)
	s.kvData = append(s.kvData, value...)

	// 键已存在，更新值与偏移位置
	if b {
		s.kvSize += lenv - s.kvNode[n+nVal]
		s.kvNode[n] = keyStart
		s.kvNode[n+nVal] = lenv
		return
	}

	// 生成随机高度
	h := s.randHeight()
	if h > s.maxHeight {
		for i := s.maxHeight; i < h; i++ {
			s.prevNode[i] = 0
		}
		s.maxHeight = h
	}

	n = len(s.kvNode)
	// 添加偏移到指定高度
	s.kvNode = append(s.kvNode, keyStart, lenk, lenv, h)
	for i, node := range s.prevNode[:h] {
		m := node + nNext + i
		s.kvNode = append(s.kvNode, s.kvNode[m])
		s.kvNode[m] = n
	}

	s.kvSize += lenk + lenv
}

// 获取键
func (s *SkipList) Get(key []byte) []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, b := s.getNode(key)
	if b { //找到键，返回数据
		keyStart := s.kvNode[n]
		keyLen := s.kvNode[n+nKey]
		valueLen := s.kvNode[n+nVal]

		return s.kvData[keyStart+keyLen : keyStart+keyLen+valueLen]
	} else {
		return nil
	}

}

// 获取区间[)数据
func (s *SkipList) GetRange(start, end []byte) []*utils.KvPair {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ret := make([]*utils.KvPair, 0)
	// 获取起始、结束位置
	endNode, _ := s.getNode(end)
	node, _ := s.getNode(start)

	// 从起始位置遍历跳表到结束位置
	for node != endNode {
		keyStart := s.kvNode[node]
		keyEnd := keyStart + s.kvNode[node+nKey]
		valueEnd := keyEnd + s.kvNode[node+nVal]

		ret = append(ret, &utils.KvPair{
			Key:   s.kvData[keyStart:keyEnd],
			Value: s.kvData[keyEnd:valueEnd],
		})
		node = s.kvNode[node+nNext]
	}

	return ret
}

// 获取区间[)数据
func (s *SkipList) GetRangeWithFilter(start, end []byte, filter func([]byte, []byte) (bool, error)) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 获取起始、结束位置
	endNode, _ := s.getNode(end)
	node, _ := s.getNode(start)

	// 从起始位置遍历跳表到结束位置
	for node != endNode {
		keyStart := s.kvNode[node]
		keyEnd := keyStart + s.kvNode[node+nKey]
		valueEnd := keyEnd + s.kvNode[node+nVal]

		complete, err := filter(s.kvData[keyStart:keyEnd], s.kvData[keyEnd:valueEnd])
		if err != nil || complete {
			return true, err
		}

		node = s.kvNode[node+nNext]
	}
	return false, nil
}

// 获取最小键
func (s *SkipList) GetMin() (key, value []byte) {
	node := s.kvNode[nNext]
	if node != 0 {
		keyStart := s.kvNode[node]
		keyEnd := keyStart + s.kvNode[node+nKey]
		valueEnd := keyEnd + s.kvNode[node+nVal]

		key = s.kvData[keyStart:keyEnd]
		value = s.kvData[keyEnd:valueEnd]
	}
	return
}

// 获取最大键
func (s *SkipList) GetMax() (key, value []byte) {
	n := 0
	h := s.maxHeight - 1

	for {
		next := s.kvNode[n+nNext+h]

		if next != 0 {
			n = next
		} else {
			s.prevNode[h] = n
			if h > 0 {
				h--
			} else {
				keyStart := s.kvNode[n]
				keyEnd := keyStart + s.kvNode[n+nKey]
				valueEnd := keyEnd + s.kvNode[n+nVal]

				key = s.kvData[keyStart:keyEnd]
				value = s.kvData[keyEnd:valueEnd]
				return
			}
		}
	}
}

// 跳表数据长度
func (s *SkipList) Size() int {
	return s.kvSize
}

func NewSkipList() *SkipList {

	s := &SkipList{
		rand:      rand.New(rand.NewSource(0xdeadbeef)),
		maxHeight: 1,
		kvData:    make([]byte, 0),
		kvNode:    make([]int, 4+tMaxHeight),
	}

	s.kvNode[nHeight] = tMaxHeight

	return s
}

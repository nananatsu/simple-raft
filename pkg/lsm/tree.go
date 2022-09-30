package lsm

import (
	"bytes"
	"fmt"
	"io/fs"
	"kvdb/pkg/skiplist"
	"kvdb/pkg/utils"
	"math"
	"os"
	"path"
	"regexp"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// sst命名正则
var SstFileNameReg = regexp.MustCompile(`^(?P<level>\d+)_(?P<seqNo>\d+)_(?P<extra>.*)\.sst$`)

type Config struct {
	Dir                 string
	Logger              *zap.SugaredLogger
	MaxLevel            int // 最大层数
	SstSize             int // sst文件大小
	SstDataBlockSize    int // sst数据块大小
	SstFooterSize       int // sst尾大小
	SstBlockTrailerSize int // sst块尾大小
	SstRestartInterval  int // sst重启点间隔
}

func NewConfig(dir string, logger *zap.SugaredLogger) *Config {
	return &Config{
		Dir:                 dir,
		Logger:              logger,
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
	conf    *Config
	tree    [][]*Node     // lsm
	seqNo   []int         // 各层sst文件最新序号
	compacc chan int      // 合并通知通道
	stopc   chan struct{} // 停止通知通道
	logger  *zap.SugaredLogger
}

// 关闭
func (t *Tree) Close() {
	for {
		select {
		case t.stopc <- struct{}{}:
		case <-time.After(time.Second):
			close(t.stopc)
			close(t.compacc)
			for _, level := range t.tree {
				for _, n := range level {
					n.Close()
				}
			}
			return
		}
	}
}

// 移动外部sst文件到指定层
func (t *Tree) Merge(level int, extra string, filePath string) error {
	// 强制合并指定层之前数据
	if level > 0 && level < t.conf.MaxLevel {
		for i := 0; i < level; i++ {
			for len(t.tree[i]) > 0 {
				err := t.compaction(i)
				if err != nil {
					return err
				}
			}
		}
	}
	// 移动&重命名文件
	newFile := formatName(level, t.NextSeqNo(level), extra)
	os.Rename(filePath, path.Join(t.conf.Dir, newFile))

	// 加载文件数据
	t.LoadNode(newFile)

	return nil
}

// 将数据写入lsm树第一层
func (t *Tree) FlushRecord(sl *skiplist.SkipListIter, extra string) error {
	level := 0
	seqNo := t.NextSeqNo(level)

	file := formatName(level, seqNo, extra)
	w, err := NewSstWriter(file, t.conf, t.logger)
	if err != nil {
		return fmt.Errorf("创建sst writer失败: %v", err)
	}
	defer w.Close()

	// 遍历跳表，将键值对写入sst文件
	count := 0
	for sl.Next() {
		w.Append(sl.Key, sl.Value)
		count++
	}

	t.logger.Infof("写入: %s ,数据数: %d ", file, count)
	// 完成写入
	size, filter, index := w.Finish()

	// 添加节点到内存lsm
	node, err := NewNode(level, seqNo, extra, file, size, filter, index, t.conf)
	if err != nil {
		return fmt.Errorf("创建lsm节点失败: %v", err)
	}
	t.insertNode(node)
	// 检查level是否合并
	t.compacc <- level

	return nil
}

// 将lsm节点放入树
func (t *Tree) insertNode(node *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 按序号将节点插入合适位置
	level := node.Level
	length := len(t.tree[level]) - 1
	idx := length
	for ; idx >= 0; idx-- {
		if node.SeqNo > t.tree[level][idx].SeqNo {
			break
		} else if node.SeqNo == t.tree[level][idx].SeqNo {
			t.tree[level][idx] = node
			return
		}
	}

	if idx == length {
		t.tree[level] = append(t.tree[level], node)
	} else {
		var newLevel []*Node
		if idx == -1 {
			newLevel = make([]*Node, 1)
			newLevel = append(newLevel, t.tree[level]...)
		} else {
			newLevel = append(t.tree[level][:idx+1], t.tree[level][idx:]...)
		}
		newLevel[idx+1] = node
		t.tree[level] = newLevel
	}
}

// 加载节点到lsm树
func (t *Tree) LoadNode(file string) error {

	var level int
	var seqNo int
	var extra string

	// 正则匹配文件名
	match := SstFileNameReg.FindStringSubmatch(file)
	if len(match) == 0 {
		return fmt.Errorf("%s 非sst", file)
	}

	// 检查正则匹配结果获取各分组数据
	for i, name := range SstFileNameReg.SubexpNames() {
		if i != 0 && name != "" {
			switch name {
			case "level":
				level, _ = strconv.Atoi(match[i])
			case "seqNo":
				seqNo, _ = strconv.Atoi(match[i])
			case "extra":
				extra = match[i]
			}
		}
	}

	// 从文件还原节点数据
	if level < t.conf.MaxLevel {
		node, err := RestoreNode(level, seqNo, extra, file, t.conf)
		if err != nil {
			return fmt.Errorf("还原lsm节点失败: %v", err)
		}

		// t.logger.Debugf("节点 %d_%d 起始 %v 结束 %v", level, seqNo, node.startKey, node.endKey)

		if t.seqNo[level] < seqNo {
			t.seqNo[level] = seqNo
		}
		t.insertNode(node)
	} else {
		return fmt.Errorf("%s 层数大于最大层数", file)
	}
	return nil
}

// 移除节点（合并后移除旧节点）
func (t *Tree) removeNode(nodes []*Node) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, node := range nodes {
		t.logger.Debugf("移除: %d_%d_%s.sst ", node.Level, node.SeqNo, node.Extra)
		for i, tn := range t.tree[node.Level] {
			if tn.SeqNo == node.SeqNo {
				t.tree[node.Level] = append(t.tree[node.Level][:i], t.tree[node.Level][i+1:]...)
				break
			}
		}
		// node.destory()
	}

	go func() {
		for _, n := range nodes {
			n.destory()
		}
	}()
}

// 取得指定层下一文件序号
func (t *Tree) NextSeqNo(level int) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seqNo[level]++

	return t.seqNo[level]
}

// 合并节点
func (t *Tree) compaction(level int) error {

	// 获取需合并节点
	nodes := t.PickupCompactionNode(level)
	lenNodes := len(nodes)
	if lenNodes == 0 {
		return nil
	}

	// 创建新节点写入
	nextLevel := level + 1
	seqNo := t.NextSeqNo(nextLevel)
	extra := nodes[lenNodes-1].Extra
	file := formatName(nextLevel, seqNo, extra)
	writer, err := NewSstWriter(file, t.conf, t.logger)

	if err != nil {
		t.logger.Errorf("%s 创建writer失败,无法合并lsm日志:%v", file, err)
		return err
	}

	var record *Record
	var files string
	maxNodeSize := t.conf.SstSize * int(math.Pow10(nextLevel))
	writeCount := 0

	// 从各节点填充数据到链表
	for i, node := range nodes {
		files += fmt.Sprintf("%d_%d_%s.sst ", node.Level, node.SeqNo, node.Extra)
		record = record.Fill(nodes, i)
	}
	t.logger.Debugf("合并: %v", files)

	// 遍历链表归并节点数据到新节点
	for record != nil {
		writeCount++
		// 写入数据
		i := record.Idx
		writer.Append(record.Key, record.Value)
		// 从消费了数据的节点填充数据
		record = record.next.Fill(nodes, i)

		// 节点数据大于当前层节点大小，完成节点节点，新建节点再写入
		if writer.Size() > maxNodeSize {
			size, filter, index := writer.Finish()
			writer.Close()

			// 添加新节点到树
			t.logger.Infof("写入: %s, 数据数: %d ", file, writeCount)
			node, err := NewNode(nextLevel, seqNo, extra, file, size, filter, index, t.conf)
			if err != nil {
				return fmt.Errorf("创建lsm节点失败: %v", err)
			}
			t.insertNode(node)

			writeCount = 0
			// 创建新节点写入
			seqNo = t.NextSeqNo(nextLevel)
			file = formatName(nextLevel, seqNo, extra)
			writer, err = NewSstWriter(file, t.conf, t.logger)
			if err != nil {
				t.logger.Errorf("%s 创建writer失败,无法合并lsm日志:%v", file, err)
				return err
			}
		}

	}

	//完成节点写入
	size, filter, index := writer.Finish()
	t.logger.Infof("写入: %s, 数据数: %d ", file, writeCount)
	// 添加到树
	node, err := NewNode(nextLevel, seqNo, extra, file, size, filter, index, t.conf)
	if err != nil {
		return fmt.Errorf("创建lsm节点失败: %v", err)
	}
	t.insertNode(node)
	t.removeNode(nodes)
	// 检查是否继续合并
	t.compacc <- nextLevel

	return nil
}

// 找出可合并节点
func (t *Tree) PickupCompactionNode(level int) []*Node {
	t.mu.Lock()
	defer t.mu.Unlock()

	compactionNode := make([]*Node, 0)

	if len(t.tree[level]) == 0 {
		return compactionNode
	}

	// 第0层 ，第一个节点起始，结束键作为检查条件
	startKey := t.tree[level][0].startKey
	endKey := t.tree[level][0].endKey
	// 其他层，取前半节点起始，结束键作为检查条件
	if level != 0 {
		node := t.tree[level][(len(t.tree[level])-1)/2]
		if bytes.Compare(node.startKey, startKey) < 0 {
			startKey = node.startKey
		}
		if bytes.Compare(node.endKey, endKey) > 0 {
			endKey = node.endKey
		}
	}

	// 检查与当前层、下一层有数据交叉的节点
	for i := level + 1; i >= level; i-- {
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

// 检查节点以进行合并
func (t *Tree) CheckCompaction() {

	level0 := make(chan struct{}, 100)
	levelN := make(chan int, 100)

	// 第0层合并协程
	go func() {
		for {
			select {
			case <-level0:
				if len(t.tree[0]) > 4 {
					t.logger.Infof("Level 0 执行合并, 当前数量: %d", len(t.tree[0]))
					t.compaction(0)
				}
			case <-t.stopc:
				close(level0)
				return
			}

		}
	}()

	// 非0层合并协程
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

					if totalSize > maxNodeSize && (prevSize == 0 || totalSize < prevSize) {
						t.logger.Infof("Level %d 当前大小: %d M, 最大大小: %d M, 执行合并", lv, totalSize/(1024*1024), maxNodeSize/(1024*1024))
						t.compaction(lv)
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

	// 合并通知处理
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

// 返回lsm树数组表示
func (t *Tree) GetNodes() [][]*Node {
	nodes := make([][]*Node, len(t.tree))
	for i, l := range t.tree {
		level := make([]*Node, len(l))
		copy(level, l)
		nodes[i] = level
	}
	return nodes
}

// 获取指定key对应value
func (t *Tree) Get(key []byte) []byte {

	for _, nodes := range t.tree {
		for i := len(nodes) - 1; i >= 0; i-- {
			value, err := nodes[i].Get(key)
			if value != nil {
				return value
			} else if err != nil {
				t.logger.Errorf("获取key: %s 对应值失败: %v", string(value), err)
			}
		}
	}
	return nil
}

// 获取区间数据
func (t *Tree) GetRange(start, end []byte) []*utils.KvPair {

	ret := make([]*utils.KvPair, 0)
	//倒序取得各节点区间数据
	for i := len(t.tree) - 1; i >= 0; i-- {
		nodes := t.tree[i]
		for i := len(nodes) - 1; i >= 0; i-- {
			rangec := nodes[i].GetRange(start, end)
			if rangec != nil {
				for value := range rangec {
					ret = append(ret, value)
					// 数据超过1000直接返回
					if len(ret) > 1000 {
						close(rangec)
						return ret
					}
				}
			}
		}
	}
	return ret
}

// 获取最小key
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

// 获取最大key
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

// 从存储还原lsm树
func RestoreTree(conf *Config) (*Tree, error) {

	lt := NewTree(conf)
	callbacks := []func(string, fs.FileInfo){
		func(name string, fileInfo fs.FileInfo) {
			err := lt.LoadNode(name)
			if err != nil {
				conf.Logger.Errorf("加载文件%s 到lsm树失败: %v", name, err)
			}
		},
	}

	if err := utils.CheckDir(conf.Dir, callbacks); err != nil {
		return lt, fmt.Errorf("还原LSM Tree状态失败: %v", err)
	}

	return lt, nil
}

// 新建lsm树
func NewTree(conf *Config) *Tree {

	compactionChan := make(chan int, 100)
	levelTree := make([][]*Node, conf.MaxLevel)

	for i := range levelTree {
		levelTree[i] = make([]*Node, 0)
	}

	seqNos := make([]int, conf.MaxLevel)

	lt := &Tree{
		conf:    conf,
		tree:    levelTree,
		seqNo:   seqNos,
		compacc: compactionChan,
		stopc:   make(chan struct{}),
		logger:  conf.Logger,
	}

	lt.CheckCompaction()

	return lt
}

// 记录链表，归并多个节点数据
type Record struct {
	Key   []byte  // 键
	Value []byte  // 值
	Idx   int     // 数据来源数组下标
	next  *Record // 下一数据位置
}

// 从指定节点填充数据到链表
func (r *Record) Fill(source []*Node, idx int) *Record {
	record := r
	// 读取节点数据
	k, v := source[idx].nextRecord()
	if k != nil {
		// 添加数据到链表
		record, idx = record.push(k, v, idx)

		//	如存在键被替换，重新填充被替换来源数据
		for idx > -1 {
			k, v := source[idx].nextRecord()
			if k != nil {
				record, idx = record.push(k, v, idx)
			} else {
				idx = -1
			}
		}
	}
	return record
}

// 添加数据到链表，返回链表头、原链表受影响数据来源下标(同名键替换)
func (r *Record) push(key, value []byte, idx int) (*Record, int) {

	h := r
	cur := r
	var prev *Record
	for {
		if cur == nil {
			// 添加数据到链表尾
			if prev != nil {
				prev.next = &Record{Key: key, Value: value, Idx: idx}
			} else { // 添加数据到链表头
				h = &Record{Key: key, Value: value, Idx: idx}
			}
			break
		}

		cmp := bytes.Compare(key, cur.Key)
		// 链表存在相同键数据,保留更新来源数据
		if cmp == 0 {
			// 节点按旧到新排序，下标更大表示数据更新
			if idx >= r.Idx {
				old := cur.Idx
				cur.Key = key
				cur.Value = value
				cur.Idx = idx
				return h, old
			}
			break
		} else if cmp < 0 { // 新增键小于当前位置键，已找到目标位置插入
			if prev != nil {
				prev.next = &Record{Key: key, Value: value, Idx: idx}
				prev.next.next = cur
			} else {
				h = &Record{Key: key, Value: value, Idx: idx}
				h.next = cur
			}
			break
		} else { // 新增键大于当前位置键，继续查找
			prev = cur
			cur = cur.next
		}
	}
	return h, -1
}

// 格式化sst文件名
func formatName(level, seqNo int, extra string) string {
	return fmt.Sprintf("%d_%d_%s.sst", level, seqNo, extra)
}

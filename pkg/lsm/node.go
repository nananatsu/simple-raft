package lsm

import (
	"bytes"
	"fmt"
	"io"
	"kvdb/pkg/filter"
	"kvdb/pkg/utils"
	"sync"

	"go.uber.org/zap"
)

type RawNodeData struct {
	Level  int
	SeqNo  int
	Offset int64
	Data   []byte
	Done   bool
	Err    error
}

type Node struct {
	wg         sync.WaitGroup    // 等待外部读取文件完成
	sr         *SstReader        // sst文件读取
	filter     map[uint64][]byte // 布隆过滤器
	startKey   []byte            // 起始键
	endKey     []byte            // 结束键
	index      []*Index          // 索引数组
	Level      int               //lsm层
	SeqNo      int               // lsm 节点序号
	Extra      string            // 文件额外信息,手动添加
	FileSize   int64             //文件大小
	compacting bool              //已在合并处理
	// 遍历节点数据用
	curBlock int           // 当前读取块
	curBuf   *bytes.Buffer // 当前读取到缓冲
	prevKey  []byte        // 前次读取到键
	logger   *zap.SugaredLogger
}

// 关闭节点
func (n *Node) Close() {
	n.sr.Close()
}

// 遍历节点数据
func (n *Node) nextRecord() ([]byte, []byte) {

	// 当前缓冲数据为空，加载数据
	if n.curBuf == nil {
		// 读取完成
		if n.curBlock > len(n.index)-1 {
			return nil, nil
		}

		// 读取数据块
		data, err := n.sr.ReadBlock(n.index[n.curBlock].Offset, n.index[n.curBlock].Size)
		if err != nil {
			if err != io.EOF {
				n.logger.Errorf("%s 读取data block失败: %v", n.Level, n.SeqNo, err)
			}
			return nil, nil
		}

		// 解析记录缓冲，更新相关属性
		record, _ := DecodeBlock(data)
		n.curBuf = bytes.NewBuffer(record)
		n.prevKey = make([]byte, 0)
		n.curBlock++
	}

	// 读取记录
	key, value, err := ReadRecord(n.prevKey, n.curBuf)
	if err == nil {
		n.prevKey = key
		return key, value
	}

	if err != io.EOF {
		n.logger.Errorf("%s 读取记录失败: %v", n.Level, n.SeqNo, err)
		return nil, nil
	}

	// 当前缓冲读取完成，加载下一缓冲
	n.curBuf = nil
	return n.nextRecord()
}

// 获取指定键对应数据
func (n *Node) Get(key []byte) ([]byte, error) {
	if bytes.Compare(key, n.startKey) < 0 || bytes.Compare(key, n.endKey) > 0 {
		return nil, nil
	}
	// 倒序遍历索引
	for _, index := range n.index[1:] {
		// 布隆过滤器检查未包含数据，直接检查后续索引
		f := n.filter[index.Offset]
		if !filter.Contains(f, key) {
			continue
		}

		// 查找键小于等于索引键，数据可能在索引对应数据块
		if bytes.Compare(key, index.Key) <= 0 {
			// 读取数据块
			data, err := n.sr.ReadBlock(index.Offset, index.Size)
			if err != nil {
				if err != io.EOF {
					return nil, fmt.Errorf("%d层 %d 节点, 读取记录失败: %v", n.Level, n.SeqNo, err)
				}
				return nil, nil
			}
			// 解析记录、重启点
			record, restartPoint := DecodeBlock(data)
			prevOffset := restartPoint[len(restartPoint)-1]
			// 倒序遍历重启点
			for i := len(restartPoint) - 2; i >= 0; i-- {
				// 按重启点偏移取得记录数据
				recordBuf := bytes.NewBuffer(record[restartPoint[i]:prevOffset])
				rKey, value, err := ReadRecord(nil, recordBuf)
				if err != nil {
					if err != io.EOF {
						return nil, fmt.Errorf("%d层 %d 节点, 读取key/value数据失败: %v", n.Level, n.SeqNo, err)
					}
					continue
				}

				cmp := bytes.Compare(key, rKey)
				// 当前记录键大于查找键，查找键在当前重启点前
				if cmp < 0 {
					prevOffset = restartPoint[i]
					continue
				} else if cmp == 0 { // 键相等，找到对应数据
					return value, nil
				} else { // 当前记录键小于查找键,查找键在当前重启点后
					prevKey := rKey
					// 遍历重启点后续记录
					for {
						rKey, value, err = ReadRecord(prevKey, recordBuf)
						if err != nil {
							if err != io.EOF {
								return nil, fmt.Errorf("%d层 %d 节点, 读取key/value数据失败: %v", n.Level, n.SeqNo, err)
							}
							return nil, nil
						}

						if bytes.Equal(key, rKey) {
							return value, nil
						}
						prevKey = rKey
					}
				}
			}
		}
	}
	return nil, nil
}

// 从指定索引开始扫描数据，返回满足区间[）条件数据
func (n *Node) scan(startIdx int, start, end []byte) chan *utils.KvPair {
	scanc := make(chan *utils.KvPair)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				n.logger.Errorf("扫描节点数据失败: %v", err)
			}
			close(scanc)
		}()

		for startIdx < len(n.index) {
			index := n.index[startIdx]
			startIdx++
			// 分隔索引第一条size会为0跳过
			if index.Size == 0 {
				continue
			}

			// 按索引记录读取数据块
			data, err := n.sr.ReadBlock(index.Offset, index.Size)
			if err != nil {
				if err != io.EOF {
					n.logger.Errorf("%d层 %d 节点, 读取记录失败: %v", n.Level, n.SeqNo, err)
				}
				return
			}

			// 解析数据块为 记录、重启点数组
			record, restartPoint := DecodeBlock(data)
			restartPointLen := len(restartPoint)

			// 遍历重启点，自重启点读取数据
			for i := 0; i < restartPointLen; i++ {
				var recordBuf *bytes.Buffer
				// 按重启点偏移切分数据
				if i+1 < restartPointLen {
					recordBuf = bytes.NewBuffer(record[restartPoint[i]:restartPoint[i+1]])
				} else {
					recordBuf = bytes.NewBuffer(record[restartPoint[i]:])
				}

				// 读取记录，重启点第一条数据键完整
				rKey, value, err := ReadRecord(nil, recordBuf)
				if err != nil {
					if err != io.EOF {
						n.logger.Errorf("%d层 %d 节点, 读取key/value数据失败: %v", n.Level, n.SeqNo, err)
					}
					return
				}

				// 当前键大于起始键，发送通道
				if bytes.Compare(start, rKey) <= 0 {
					scanc <- &utils.KvPair{Key: rKey, Value: value}
				}

				// 遍历重启点后续记录，后续数据与前次记录共享部分键
				prevKey := rKey
				for {
					rKey, value, err = ReadRecord(prevKey, recordBuf)
					if err != nil {
						if err != io.EOF {
							n.logger.Errorf("%d层 %d 节点, 读取key/value数据失败: %v", n.Level, n.SeqNo, err)
							return
						}
						// 当前重启点读取完成
						break
					}

					// 结束键小于等于当前键，结束扫描
					if bytes.Compare(end, rKey) <= 0 {
						return
					} else if bytes.Compare(start, rKey) <= 0 { // 键包含在区间
						scanc <- &utils.KvPair{Key: rKey, Value: value}
					}
					prevKey = rKey
				}
			}
		}
	}()

	return scanc
}

// 获取指定区间[)数据
func (n *Node) GetRange(start, end []byte) chan *utils.KvPair {
	if bytes.Compare(end, n.startKey) < 0 || bytes.Compare(start, n.endKey) > 0 {
		return nil
	}

	for i, index := range n.index {
		// n.logger.Debugf("%v <= %v : %t, %v > %v : %t", start, index.Key, bytes.Compare(start, index.Key) <= 0, end, index.Key, bytes.Compare(end, index.Key) > 0)
		if bytes.Compare(start, index.Key) <= 0 {
			if bytes.Compare(end, index.Key) > 0 {
				return n.scan(i, start, end)
			} else {
				break
			}
		}
	}
	return nil
}

// 直接读取节点对应文件
func (n *Node) ReadRaw(perSize int64) chan *RawNodeData {

	readc := make(chan *RawNodeData)
	remain := n.FileSize
	var offset int64
	var data []byte
	var err error
	var done bool

	n.wg.Add(1)
	go func() {
		defer func() {
			close(readc)
			n.wg.Done()
		}()

		for remain > 0 {
			if remain > perSize {
				data, err = n.sr.Read(offset, perSize)
			} else {
				data, err = n.sr.Read(offset, remain)
				if err == nil {
					done = true
				}
			}

			if err != nil {
				err = fmt.Errorf("读取 %d_%d_%s 数据失败: %v", n.Level, n.SeqNo, n.Extra, err)
			}

			readc <- &RawNodeData{Level: n.Level, SeqNo: n.SeqNo, Offset: offset, Data: data, Done: done, Err: err}
			if err != nil {
				break
			} else {
				readSize := int64(len(data))
				offset += readSize
				remain -= readSize
			}
		}
	}()

	return readc
}

// 销毁节点
func (n *Node) destory() {
	n.wg.Wait()
	n.sr.Destory()
	n.Level = -1
	n.filter = nil
	n.index = nil
	n.curBuf = nil
	n.prevKey = nil
	n.FileSize = 0
}

// 从文件还原节点
func RestoreNode(level, seqNo int, extra string, file string, conf *Config) (*Node, error) {

	r, err := NewSstReader(file, conf)
	if err != nil {
		return nil, fmt.Errorf("%s 创建sst Reader: %v", file, err)
	}

	filter, err := r.ReadFilter()
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("%s 读取过滤块失败: %v ", file, err)
	}

	index, err := r.ReadIndex()
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("%s 读取索引失败: %v ", file, err)
	}

	return &Node{
		sr:       r,
		Level:    level,
		SeqNo:    seqNo,
		Extra:    extra,
		curBlock: 1,
		FileSize: r.IndexOffset + r.IndexSize + int64(conf.SstFooterSize),
		filter:   filter,
		index:    index,
		startKey: index[0].Key,
		endKey:   index[len(index)-1].Key,
		logger:   conf.Logger,
	}, nil

}

// 新建节点
func NewNode(level, seqNo int, extra string, file string, size int64, filter map[uint64][]byte, index []*Index, conf *Config) (*Node, error) {
	r, err := NewSstReader(file, conf)

	if err != nil {
		return nil, fmt.Errorf("%s 创建sst Reader: %v", file, err)
	}

	return &Node{
		sr:    r,
		Level: level,
		SeqNo: seqNo,
		Extra: extra,
		// 索引从下标1开始读取，下标0无前一数据块
		curBlock: 1,
		FileSize: size,
		filter:   filter,
		index:    index,
		startKey: index[0].Key,
		endKey:   index[len(index)-1].Key,
		logger:   conf.Logger,
	}, nil
}

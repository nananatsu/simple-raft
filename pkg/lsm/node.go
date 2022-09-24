package lsm

import (
	"bytes"
	"fmt"
	"io"
	"kvdb/pkg/filter"
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
	wg         sync.WaitGroup
	sr         *SstReader
	filter     map[uint64][]byte
	startKey   []byte
	endKey     []byte
	index      []*Index
	Level      int
	SeqNo      int
	Extra      string
	FileSize   int64
	curBlock   int
	curBuf     *bytes.Buffer
	prevKey    []byte
	compacting bool
	logger     *zap.SugaredLogger
}

func (n *Node) Close() {
	n.sr.Close()
}

func (n *Node) nextRecord() ([]byte, []byte) {

	if n.curBuf == nil {
		if n.curBlock > len(n.index)-1 {
			return nil, nil
		}

		data, err := n.sr.ReadBlock(n.index[n.curBlock].Offset, n.index[n.curBlock].Size)
		if err != nil {
			if err != io.EOF {
				n.logger.Errorf("%s 读取data block失败: %v", n.Level, n.SeqNo, err)
			}
			return nil, nil
		}

		record, _ := DecodeBlock(data)
		n.curBuf = bytes.NewBuffer(record)
		n.prevKey = make([]byte, 0)
		n.curBlock++
	}

	key, value, err := ReadRecord(n.prevKey, n.curBuf)
	if err == nil {
		n.prevKey = key
		return key, value
	}

	if err != io.EOF {
		n.logger.Errorf("%s 读取记录失败: %v", n.Level, n.SeqNo, err)
		return nil, nil
	}

	n.curBuf = nil
	return n.nextRecord()
}

func (n *Node) Get(key []byte) ([]byte, error) {
	if bytes.Compare(key, n.startKey) < 0 || bytes.Compare(key, n.endKey) > 0 {
		return nil, nil
	}

	for _, index := range n.index[1:] {
		f := n.filter[index.Offset]
		if !filter.Contains(f, key) {
			continue
		}

		if bytes.Compare(key, index.Key) <= 0 {
			data, err := n.sr.ReadBlock(index.Offset, index.Size)
			if err != nil {
				if err != io.EOF {
					return nil, fmt.Errorf("%d层 %d 节点, 读取记录失败: %v", n.Level, n.SeqNo, err)
				}
				return nil, nil
			}

			record, restartPoint := DecodeBlock(data)
			prevOffset := restartPoint[len(restartPoint)-1]
			for i := len(restartPoint) - 2; i >= 0; i-- {
				recordBuf := bytes.NewBuffer(record[restartPoint[i]:prevOffset])
				rKey, value, err := ReadRecord(nil, recordBuf)
				if err != nil {
					if err != io.EOF {
						return nil, fmt.Errorf("%d层 %d 节点, 读取key/value数据失败: %v", n.Level, n.SeqNo, err)
					}
					continue
				}

				cmp := bytes.Compare(key, rKey)
				if cmp < 0 {
					prevOffset = restartPoint[i]
					continue
				} else if cmp == 0 {
					return value, nil
				} else {
					prevKey := rKey
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

func NewNode(level, seqNo int, extra string, file string, size int64, filter map[uint64][]byte, index []*Index, conf *Config) (*Node, error) {
	r, err := NewSstReader(file, conf)

	if err != nil {
		return nil, fmt.Errorf("%s 创建sst Reader: %v", file, err)
	}

	return &Node{
		sr:       r,
		Level:    level,
		SeqNo:    seqNo,
		Extra:    extra,
		curBlock: 1,
		FileSize: size,
		filter:   filter,
		index:    index,
		startKey: index[0].Key,
		endKey:   index[len(index)-1].Key,
		logger:   conf.Logger,
	}, nil
}

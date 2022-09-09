package lsm

import (
	"bytes"
	"io"
	"kvdb/pkg/filter"

	"go.uber.org/zap"
)

type Node struct {
	sr         *SstReader
	filter     map[uint64][]byte
	startKey   []byte
	endKey     []byte
	index      []*Index
	Level      int
	SeqNo      int
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

func (n *Node) Load() error {

	filter, err := n.sr.ReadFilter()
	if err != nil {
		n.logger.Errorf("%d.%d.sst 读取过滤块失败: %v ", n.Level, n.SeqNo, err)
	}

	index, err := n.sr.ReadIndex()
	if err != nil {
		n.logger.Errorf("%d.%d.sst 读取索引失败: %v ", n.Level, n.SeqNo, err)
	}

	n.filter = filter
	n.index = index
	n.startKey = n.index[0].Key
	n.endKey = n.index[len(n.index)-1].Key

	return err
}

func (n *Node) NextRecord() ([]byte, []byte) {

	if n.curBuf == nil {
		if n.curBlock > len(n.index)-1 {
			return nil, nil
		}

		data, err := n.sr.ReadBlock(n.index[n.curBlock].Offset, n.index[n.curBlock].Size)
		if err != nil {
			if err != io.EOF {
				n.logger.Errorf("读取data block失败", err)
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
		n.logger.Errorf("读取记录失败", err)
	}

	n.curBuf = nil
	return n.NextRecord()
}

func (n *Node) Get(key []byte) []byte {
	if bytes.Compare(key, n.startKey) < 0 || bytes.Compare(key, n.endKey) > 0 {
		return nil
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
					n.logger.Errorf("读取data block失败", err)
				}
				return nil
			}

			record, restartPoint := DecodeBlock(data)
			prevOffset := restartPoint[len(restartPoint)-1]
			for i := len(restartPoint) - 2; i >= 0; i-- {
				recordBuf := bytes.NewBuffer(record[restartPoint[i]:prevOffset])
				rKey, value, err := ReadRecord(nil, recordBuf)
				if err != nil {
					if err != io.EOF {
						n.logger.Errorln("读取key/value数据失败", err)
					}
					continue
				}

				cmp := bytes.Compare(key, rKey)
				if cmp < 0 {
					prevOffset = restartPoint[i]
					continue
				} else if cmp == 0 {
					return value
				} else {
					prevKey := rKey
					for {
						rKey, value, err = ReadRecord(prevKey, recordBuf)
						if err != nil {
							if err != io.EOF {
								n.logger.Errorln("读取key/value数据失败", err)
							}
							return nil
						}

						if bytes.Equal(key, rKey) {
							return value
						}
						prevKey = rKey
					}
				}

			}

		}
	}
	return nil
}

func (n *Node) Destory() {

	n.logger.Infof("移除: %d.%d.sst ", n.Level, n.SeqNo)
	n.sr.Destory()

	n.Level = -1
	n.filter = nil
	n.index = nil
	n.curBuf = nil
	n.prevKey = nil
	n.FileSize = 0
}

package lsm

import (
	"bytes"
	"kvdb/pkg/filter"
	"log"
)

type Node struct {
	sr *SstReader

	filter   []byte
	startKey []byte
	endKey   []byte
	index    []*Index
	Level    int
	SeqNo    int
	FileSize int64

	curBlock int
	curBuf   *bytes.Buffer
	prevKey  []byte

	compacting bool
}

func (n *Node) Load() error {

	filter, err := n.sr.ReadFilter()

	if err != nil {
		log.Printf("%d.%d.sst 读取过滤块失败: %v \n", n.Level, n.SeqNo, err)
	}

	index, err := n.sr.ReadIndex()

	if err != nil {
		log.Printf("%d.%d.sst 读取索引失败: %v \n", n.Level, n.SeqNo, err)
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

	n.curBuf = nil
	return n.NextRecord()
}

func (n *Node) HasKey(key []byte) bool {

	if !filter.Contains(n.filter, key) {
		return false
	}

	if bytes.Compare(key, n.startKey) >= 0 && bytes.Compare(key, n.endKey) <= 0 {
		return true
	}

	return false
}

func (n *Node) Get(key []byte) []byte {

	if !n.HasKey(key) {
		return nil
	}

	for _, indexMeata := range n.index[1:] {
		if bytes.Compare(key, indexMeata.Key) <= 0 {
			data, err := n.sr.ReadBlock(indexMeata.Offset, indexMeata.Size)
			if err != nil {
				return nil
			}

			record, restartPoint := DecodeBlock(data)
			prevOffset := restartPoint[len(restartPoint)-1]

			for i := len(restartPoint) - 2; i >= 0; i-- {

				recordBuf := bytes.NewBuffer(record[restartPoint[i]:prevOffset])

				rKey, value, err := ReadRecord(nil, recordBuf)

				if err != nil {
					log.Println("读取key/value数据失败", err)
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
							// log.Println("读取key/value数据失败", err)
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

	log.Printf("移除: %d.%d.sst \n", n.Level, n.SeqNo)
	n.sr.Destory()

	n.Level = -1
	n.filter = nil
	n.index = nil
	n.curBuf = nil
	n.prevKey = nil
	n.FileSize = 0
}

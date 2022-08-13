package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"kvdb/pkg/filter"
	"kvdb/pkg/memtable/skiplist"
	"os"
)

func SharedPrefixLen(a, b []byte) int {
	i, n := 0, len(a)
	if n > len(b) {
		n = len(b)
	}
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

const dataBlockSize = 4 * 1024

type ImmutableMemTable struct {
	fd              *os.File
	memTable        *skiplist.SkipList
	buf             *bytes.Buffer
	indexBuf        *bytes.Buffer
	restartInterval int
}

func (t *ImmutableMemTable) FlushMemTable() {

	it := skiplist.NewSkipListIter(t.memTable)
	bf := filter.NewBloomFilter(10)

	header := make([]byte, 30)
	trailer := make([]byte, 0)

	prevKey := make([]byte, 0)

	nRestartPoint := 0
	nEntries := 0

	for it.Next() {

		keyLen := len(it.Key)
		valueLen := len(it.Value)

		nSharePrefix := 0

		// restart point
		if nEntries%t.restartInterval == 0 {
			buf4 := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf4, uint32(len(t.buf.Bytes())))
			trailer = append(trailer, buf4...)
			nRestartPoint++
			// o := binary.PutUvarint(index[0:], uint64(keyLen))
			// t.indexBuf.Write(index[:o])
			// t.indexBuf.Write(it.Key)

			// o = binary.PutUvarint(index[0:], uint64(len(t.buf.Bytes())))
			// t.indexBuf.Write(index[:o])

		} else {
			nSharePrefix = SharedPrefixLen(prevKey, it.Key)
		}

		n := binary.PutUvarint(header[0:], uint64(nSharePrefix))
		n += binary.PutUvarint(header[n:], uint64(keyLen-nSharePrefix))
		n += binary.PutUvarint(header[n:], uint64(valueLen))

		// data
		t.buf.Write(header[:n])
		t.buf.Write(it.Key[nSharePrefix:])
		t.buf.Write(it.Value)

		prevKey = append(prevKey[:0], it.Key...)
		nEntries++

		bf.Add(it.Key)

		if len(t.buf.Bytes()) > dataBlockSize {

			buf4 := make([]byte, 4)
			binary.LittleEndian.PutUint32(buf4, uint32(nRestartPoint))
			trailer = append(trailer, buf4...)

		}
	}

	footer := make([]byte, 40)

	data := t.buf.Bytes()
	dataSize := len(data)

	filterBlock := bf.Hash()
	filterSize := len(filterBlock)

	indexBlock := t.indexBuf.Bytes()
	indexSize := len(indexBlock)

	// metadata 稀疏索引起始偏移，整体长度
	n := binary.PutUvarint(footer[0:], uint64(dataSize))
	n += binary.PutUvarint(footer[n:], uint64(dataSize+filterSize))
	n += binary.PutUvarint(footer[n:], uint64(indexSize))

	t.fd.Write(data)
	t.fd.Write(filterBlock)
	t.fd.Write(indexBlock)
	t.fd.Write(footer)

}

func NewImmutableMemTable(sstFilePath string, memTable *skiplist.SkipList, restartInterval int) *ImmutableMemTable {

	fd, err := os.OpenFile(sstFilePath, os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		fmt.Println("打开db文件失败", err)
	}

	buf := make([]byte, 0)
	indexBuf := make([]byte, 0)

	return &ImmutableMemTable{
		fd:              fd,
		memTable:        memTable,
		buf:             bytes.NewBuffer(buf),
		indexBuf:        bytes.NewBuffer(indexBuf),
		restartInterval: restartInterval,
	}
}

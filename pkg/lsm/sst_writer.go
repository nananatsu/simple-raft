package lsm

import (
	"bytes"
	"encoding/binary"
	"kvdb/pkg/filter"
	"log"
	"os"
)

const dataBlockSize = 16 * 1024
const footerLen = 40

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

func GetSeparator(a, b []byte) []byte {
	n := SharedPrefixLen(a, b)

	var sep []byte

	if n == 0 || n == len(a) {
		sep = a
	} else {
		c := a[n] + 1
		sep = append(a[0:n], c)
	}

	return sep
}

type SstWriter struct {
	fd *os.File

	dataBuf   *bytes.Buffer
	filterBuf *bytes.Buffer
	indexBuf  *bytes.Buffer

	index  []*Index
	filter map[uint64][]byte

	bf          *filter.BloomFilter
	dataBlock   *Block
	filterBlock *Block
	indexBlock  *Block

	indexScratch [20]byte
	prevKey      []byte

	prevBlockOffset uint64
	prevBlockSize   uint64
}

func (w *SstWriter) addIndex(key []byte) {

	n := binary.PutUvarint(w.indexScratch[0:], w.prevBlockOffset)
	n += binary.PutUvarint(w.indexScratch[n:], w.prevBlockSize)
	w.indexBlock.Append(w.prevKey, w.indexScratch[:n])
	separator := GetSeparator(w.prevKey, key)

	w.index = append(w.index, &Index{Key: separator, Offset: w.prevBlockOffset, Size: w.prevBlockSize})
}

func (w *SstWriter) Append(key, value []byte) {

	if w.dataBlock.nEntries == 0 {
		w.addIndex(key)
	}

	w.dataBlock.Append(key, value)
	w.bf.Add(key)

	w.prevKey = key

	if w.dataBlock.Size() > dataBlockSize {
		var err error
		w.prevBlockOffset = uint64(w.dataBuf.Len())
		n := binary.PutUvarint(w.indexScratch[0:], uint64(w.prevBlockOffset))

		filter := w.bf.Hash()
		w.filter[w.prevBlockOffset] = filter
		w.filterBlock.Append(w.indexScratch[:n], filter)
		w.bf.Reset()

		w.prevBlockSize, err = w.dataBlock.FlushBlockTo(w.dataBuf)

		if err != nil {
			log.Println("写入block失败", err)
		}
	}
}

func (w *SstWriter) Finish() (int64, map[uint64][]byte, []*Index) {

	if w.bf.KeyLen() > 0 {
		n := binary.PutUvarint(w.indexScratch[0:], uint64(w.prevBlockOffset))
		w.filterBlock.Append(w.indexScratch[:n], w.bf.Hash())
	}
	w.filterBlock.FlushBlockTo(w.filterBuf)

	w.addIndex(w.prevKey)
	w.indexBlock.FlushBlockTo(w.indexBuf)

	footer := make([]byte, footerLen)

	size := w.dataBuf.Len()

	// metadata 索引起始偏移，整体长度
	n := binary.PutUvarint(footer[0:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.filterBuf.Len()))
	size += w.filterBuf.Len()
	n += binary.PutUvarint(footer[n:], uint64(size))
	n += binary.PutUvarint(footer[n:], uint64(w.indexBuf.Len()))
	size += w.indexBuf.Len()
	size += footerLen

	w.fd.Write(w.dataBuf.Bytes())
	w.fd.Write(w.filterBuf.Bytes())
	w.fd.Write(w.indexBuf.Bytes())
	w.fd.Write(footer)

	return int64(size), w.filter, w.index
}

func (w *SstWriter) Size() int {
	// return w.dataBuf.Len() + w.indexBuf.Len() + w.bf.Size()
	return w.dataBuf.Len()
}

func (w *SstWriter) Close() {
	w.fd.Close()
	w.dataBuf.Reset()
	w.indexBuf.Reset()
}

func NewSstWriter(fd *os.File) *SstWriter {

	return &SstWriter{
		fd:          fd,
		dataBuf:     bytes.NewBuffer(make([]byte, 0)),
		filterBuf:   bytes.NewBuffer(make([]byte, 0)),
		indexBuf:    bytes.NewBuffer(make([]byte, 0)),
		filter:      make(map[uint64][]byte),
		index:       make([]*Index, 0),
		bf:          filter.NewBloomFilter(10),
		dataBlock:   NewBlock(),
		filterBlock: NewBlock(),
		indexBlock:  NewBlock(),
		prevKey:     make([]byte, 0),
	}
}

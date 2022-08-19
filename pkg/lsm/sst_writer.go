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
	indexBuf  *bytes.Buffer
	indexMeta []*Index

	data  *Block
	bf    *filter.BloomFilter
	index *Block

	indexScratch [20]byte
	prevKey      []byte

	prevBlockOffset int
	prevBlockSize   int
}

func (w *SstWriter) addIndex(key []byte) {

	n := binary.PutUvarint(w.indexScratch[0:], uint64(w.prevBlockOffset))
	n += binary.PutUvarint(w.indexScratch[n:], uint64(w.prevBlockSize))
	w.index.Append(w.prevKey, w.indexScratch[:n])
	separator := GetSeparator(w.prevKey, key)

	w.indexMeta = append(w.indexMeta, &Index{Key: separator, Offset: int64(w.prevBlockOffset), Size: int64(w.prevBlockSize)})
}

func (w *SstWriter) Append(key, value []byte) {

	if w.data.nEntries == 0 {
		w.addIndex(key)
	}

	w.data.Append(key, value)
	w.bf.Add(key)

	w.prevKey = key

	if w.data.Size() > dataBlockSize {
		var err error
		w.prevBlockOffset = w.dataBuf.Len()
		w.prevBlockSize, err = w.data.FlushBlockTo(w.dataBuf)

		if err != nil {
			log.Println("写入block失败", err)
		}
	}
}

func (w *SstWriter) Finish() (int64, []byte, []*Index) {

	w.addIndex(w.prevKey)
	w.index.FlushBlockTo(w.indexBuf)

	footer := make([]byte, footerLen)

	size := w.dataBuf.Len()
	filterBlock := w.bf.Hash()

	// metadata 索引起始偏移，整体长度
	n := binary.PutUvarint(footer[0:], uint64(size))
	size += len(filterBlock)
	n += binary.PutUvarint(footer[n:], uint64(size))
	size += w.indexBuf.Len()
	n += binary.PutUvarint(footer[n:], uint64(w.indexBuf.Len()))
	size += footerLen

	w.fd.Write(w.dataBuf.Bytes())
	w.fd.Write(filterBlock)
	w.fd.Write(w.indexBuf.Bytes())
	w.fd.Write(footer)

	return int64(size), filterBlock, w.indexMeta
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

	buf := make([]byte, 0)
	indexBuf := make([]byte, 0)
	prevKey := make([]byte, 0)
	indexMeta := make([]*Index, 0)

	return &SstWriter{
		fd:        fd,
		dataBuf:   bytes.NewBuffer(buf),
		indexBuf:  bytes.NewBuffer(indexBuf),
		indexMeta: indexMeta,
		bf:        filter.NewBloomFilter(10),
		data:      NewBlock(),
		index:     NewBlock(),
		prevKey:   prevKey,
	}
}

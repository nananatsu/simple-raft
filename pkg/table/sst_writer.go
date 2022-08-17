package table

import (
	"bytes"
	"encoding/binary"
	"kvdb/pkg/filter"
	"log"
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

type SstWriter struct {
	fd *os.File

	dataBuf  *bytes.Buffer
	indexBuf *bytes.Buffer

	data  *Block
	bf    *filter.BloomFilter
	index *Block

	indexScratch  [20]byte
	prevKey       []byte
	prevBlockSize int
}

func (w *SstWriter) Append(key, value []byte) {

	if w.data.nEntries == 0 && len(w.prevKey) > 0 {
		n := binary.PutUvarint(w.indexScratch[0:], uint64(w.dataBuf.Len()))
		n += binary.PutUvarint(w.indexScratch[n:], uint64(w.prevBlockSize))
		w.index.Append(w.prevKey, w.indexScratch[:n])
	}

	w.data.Append(key, value)
	w.bf.Add(key)

	if w.data.Size() > dataBlockSize {

		var err error
		w.prevKey = key
		w.prevBlockSize, err = w.data.FlushBlockTo(w.dataBuf)

		if err != nil {
			log.Println("写入block失败", err)
		}

	}
}

func (w *SstWriter) Finish() {
	w.index.FlushBlockTo(w.indexBuf)

	footer := make([]byte, footerLen)

	dataSize := w.dataBuf.Len()
	filterBlock := w.bf.Hash()
	filterSize := len(filterBlock)

	// metadata 索引起始偏移，整体长度
	n := binary.PutUvarint(footer[0:], uint64(dataSize))
	n += binary.PutUvarint(footer[n:], uint64(dataSize+filterSize))
	n += binary.PutUvarint(footer[n:], uint64(w.indexBuf.Len()))

	w.fd.Write(w.dataBuf.Bytes())
	w.fd.Write(filterBlock)
	w.fd.Write(w.indexBuf.Bytes())
	w.fd.Write(footer)
}

func (w *SstWriter) Size() int {
	// return w.dataBuf.Len() + w.indexBuf.Len() + w.bf.Size()
	return w.dataBuf.Len()
}

func NewSstWriter(fd *os.File) *SstWriter {

	buf := make([]byte, 0)
	indexBuf := make([]byte, 0)
	prevKey := make([]byte, 0)

	return &SstWriter{
		fd:       fd,
		dataBuf:  bytes.NewBuffer(buf),
		indexBuf: bytes.NewBuffer(indexBuf),
		bf:       filter.NewBloomFilter(10),
		data:     NewBlock(),
		index:    NewBlock(),
		prevKey:  prevKey,
	}
}
